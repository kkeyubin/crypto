from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_research.config import Settings
from crypto_research.contracts.data import (
    AddSymbolRequest,
    BackfillRequest,
    EligibilityReasonCode,
    SymbolDataStatus,
)
from crypto_research.contracts.manifest import DataType
from crypto_research.db.repositories import (
    AddSymbolCommand,
    BackfillCommand,
    DataGap,
    DataPartition,
    IngestionJob,
    MutationIdentityConflict,
    StreamState,
    SymbolOperationalSummary,
    SymbolState,
    WorkerHeartbeat,
)
from crypto_research.market.backfill import BackfillObject
from crypto_research.market.binance.streams import streams_for_symbols
from crypto_research.market.control import (
    MarketDataConflict,
    MarketDataControlService,
    MarketDataNotFound,
    MarketDataValidationError,
)
from crypto_research.market.profile import ProfileMetric, SymbolProfile

NOW = datetime(2026, 7, 21, 12, tzinfo=UTC)
START = datetime(2026, 6, 1, tzinfo=UTC)
END = datetime(2026, 7, 1, tzinfo=UTC)


class Repository:
    def __init__(self) -> None:
        self.symbols: dict[str, SymbolState] = {}
        self.jobs: dict[str, IngestionJob] = {}
        self.partitions: dict[str, tuple[DataPartition, ...]] = {}
        self.gaps: dict[str, tuple[DataGap, ...]] = {}
        self.streams: dict[str, tuple[StreamState, ...]] = {}
        self.summaries: dict[str, SymbolOperationalSummary] = {}
        self.audit_actions: list[str] = []
        self.heartbeat: WorkerHeartbeat | None = None
        self.planned: dict[str, BackfillObject] = {}
        self.health_aggregate_calls = 0
        self.summary_calls = 0
        self.bulk_summary_calls = 0

    async def list_symbols(self, *, limit: int, offset: int):
        ordered = tuple(self.symbols[key] for key in sorted(self.symbols))
        return ordered[offset : offset + limit]

    async def list_active_symbols(self):
        return tuple(
            self.symbols[key]
            for key in sorted(self.symbols)
            if self.symbols[key].enabled
        )

    async def get_symbol(self, symbol: str):
        return self.symbols.get(symbol)

    async def add_symbol(self, command: AddSymbolCommand):
        created = SymbolState(
            command.symbol,
            True,
            command.history_start,
            command.history_end,
            command.include_agg_trades,
            NOW,
            NOW,
        )
        self.symbols[command.symbol] = created
        self.audit_actions.append("symbol_added")
        return created

    async def set_symbol_enabled(self, symbol: str, enabled: bool):
        current = self.symbols[symbol]
        if current.enabled != enabled:
            self.symbols[symbol] = replace(current, enabled=enabled, updated_at=NOW)
            self.audit_actions.append("symbol_enabled" if enabled else "symbol_disabled")
        return self.symbols[symbol]

    async def create_backfill(self, command: BackfillCommand):
        existing = self.jobs.get(command.id)
        if existing is not None:
            return existing
        created = IngestionJob(
            command.id,
            command.symbol,
            command.dataset,
            "queued",
            command.requested_start,
            command.requested_end,
            NOW,
            NOW,
        )
        self.jobs[command.id] = created
        self.audit_actions.append("backfill_created")
        return created

    async def get_backfill(self, job_id: str):
        return self.jobs.get(job_id)

    async def plan(self, work: BackfillObject):
        return self.planned.setdefault(work.object_id, work)

    async def list_partitions(self, symbol: str, *, limit: int, offset: int):
        return self.partitions.get(symbol, ())[offset : offset + limit]

    async def list_gaps(self, symbol: str, *, limit: int, offset: int):
        return self.gaps.get(symbol, ())[offset : offset + limit]

    async def list_stream_states(
        self, symbol: str | None = None, *, limit: int = 100, offset: int = 0
    ):
        values = (
            self.streams.get(symbol, ())
            if symbol is not None
            else tuple(item for key in sorted(self.streams) for item in self.streams[key])
        )
        return values[offset : offset + limit]

    async def list_active_stream_states(self):
        enabled = {symbol for symbol, state in self.symbols.items() if state.enabled}
        values = tuple(
            item
            for symbol in sorted(self.streams)
            if symbol in enabled
            for item in self.streams[symbol]
        )
        return values

    async def get_active_stream_health(
        self, *, checked_at: datetime, stale_after: timedelta
    ):
        self.health_aggregate_calls += 1
        active = await self.list_active_symbols()
        streams = await self.list_active_stream_states()
        expected_names = {
            stream.name
            for state in active
            for stream in streams_for_symbols((state.symbol,))
        }
        matching = tuple(
            stream for stream in streams if stream.stream_name in expected_names
        )
        healthy = tuple(
            stream
            for stream in matching
            if stream.status == "connected"
            and stream.last_event_at is not None
            and checked_at - stale_after <= stream.last_event_at <= checked_at
        )
        return SimpleNamespace(
            expected_count=len(expected_names),
            observed_count=len(matching),
            healthy_count=len(healthy),
        )

    async def get_symbol_summary(self, symbol: str):
        self.summary_calls += 1
        return self.summaries.get(symbol, SymbolOperationalSummary())

    async def list_symbol_summaries(self, symbols: tuple[SymbolState, ...]):
        self.bulk_summary_calls += 1
        return {
            state.symbol: self.summaries.get(
                state.symbol, SymbolOperationalSummary()
            )
            for state in symbols
        }

    async def get_worker_heartbeat(self, worker_id: str):
        del worker_id
        return self.heartbeat

    async def count_failed_jobs(self):
        return sum(item.status == "failed" for item in self.jobs.values())


class Profiles:
    async def compute(self, symbol: str, start: datetime, end: datetime, *, calculated_at):
        base = 1.0 if symbol == "BTCUSDT" else 9.0
        metric = ProfileMetric(base, 20, 0.99)
        return SymbolProfile(
            symbol=symbol,
            calculated_at=calculated_at,
            coverage_start=start,
            coverage_end=end,
            realized_volatility=metric,
            jump_frequency=metric,
            median_spread_bps=metric,
            median_hourly_volume=ProfileMetric(base * 1000, 20, 0.99),
            funding_rate_mean=metric,
        )


def configured_repository() -> Repository:
    repository = Repository()
    repository.symbols = {
        "BTCUSDT": SymbolState("BTCUSDT", True, START, END, False, NOW, NOW),
        "PEPEUSDT": SymbolState("PEPEUSDT", True, START, END, False, NOW, NOW),
    }
    repository.summaries = {
        "BTCUSDT": SymbolOperationalSummary(
            approved_data_types=("kline_1m", "mark_price", "funding"),
            metadata_verified=False,
            open_gap_count=0,
            job_statuses=("succeeded",),
        ),
        "PEPEUSDT": SymbolOperationalSummary(
            approved_data_types=("kline_1m",),
            metadata_verified=False,
            open_gap_count=1,
            job_statuses=("failed",),
        ),
    }
    return repository


def service(repository: Repository, **settings_overrides: object):
    settings = Settings(
        _env_file=None,
        allowed_hosts=["testserver"],
        **settings_overrides,
    )
    return MarketDataControlService(
        repository,
        Profiles(),
        settings,
        clock=lambda: NOW,
    )


def test_service_owns_add_conflict_reenable_and_history_limit_rules() -> None:
    async def scenario() -> None:
        repository = configured_repository()
        control = service(repository, history_max_days=30)
        same = AddSymbolRequest(
            symbol="BTCUSDT",
            history_start=START,
            history_end=END,
        )

        first = await control.add_symbol(same)
        second = await control.add_symbol(same)
        disabled = await control.disable_symbol("BTCUSDT")
        reenabled = await control.add_symbol(same)

        assert first == second
        assert disabled.enabled is False
        assert reenabled.enabled is True
        assert repository.audit_actions == ["symbol_disabled", "symbol_enabled"]

        with pytest.raises(MarketDataConflict):
            await control.add_symbol(
                same.model_copy(
                    update={"history_start": START + timedelta(days=1)}
                )
            )
        with pytest.raises(MarketDataValidationError, match="30 days"):
            await control.add_symbol(
                AddSymbolRequest(
                    symbol="ETHUSDT",
                    history_start=START,
                    history_end=END + timedelta(days=1),
                )
            )

    asyncio.run(scenario())


def test_symbol_list_uses_one_bulk_summary_projection_without_n_plus_one() -> None:
    async def scenario() -> None:
        repository = configured_repository()

        values = await service(repository).list_symbols(limit=50, offset=0)

        assert [item.symbol for item in values] == ["BTCUSDT", "PEPEUSDT"]
        assert repository.bulk_summary_calls == 1
        assert repository.summary_calls == 0

    asyncio.run(scenario())


def test_service_maps_concurrent_repository_identity_conflicts_to_domain_409() -> None:
    class ConflictingRepository(Repository):
        async def add_symbol(self, command: AddSymbolCommand):
            del command
            raise MutationIdentityConflict("concurrent symbol conflict")

    async def scenario() -> None:
        repository = ConflictingRepository()
        with pytest.raises(MarketDataConflict, match="concurrent symbol conflict"):
            await service(repository).add_symbol(
                AddSymbolRequest(
                    symbol="ETHUSDT",
                    history_start=START,
                    history_end=END,
                )
            )

    asyncio.run(scenario())


def test_service_creates_deterministic_per_dataset_jobs_and_requires_symbol_opt_in() -> None:
    async def scenario() -> None:
        repository = configured_repository()
        control = service(repository)
        request = BackfillRequest(
            symbol="BTCUSDT",
            data_types=(DataType.FUNDING, DataType.KLINE_1M),
            start=START,
            end=END,
        )

        first = await control.create_backfills("BTCUSDT", request)
        second = await control.create_backfills("BTCUSDT", request)

        assert first == second
        assert [item.data_type for item in first] == [DataType.FUNDING, DataType.KLINE_1M]
        assert len(repository.jobs) == 2
        assert len(repository.planned) == 2
        assert all(
            item.source_url.startswith("https://data.binance.vision/")
            and "?" not in item.source_url
            for item in repository.planned.values()
        )
        assert repository.audit_actions == ["backfill_created", "backfill_created"]

        with pytest.raises(MarketDataConflict, match="path symbol"):
            await control.create_backfills(
                "PEPEUSDT", request
            )
        with pytest.raises(MarketDataConflict, match="aggregate-trade"):
            await control.create_backfills(
                "BTCUSDT",
                BackfillRequest(
                    symbol="BTCUSDT",
                    data_types=(DataType.AGG_TRADE,),
                    start=START,
                    end=END,
                    include_agg_trades=True,
                ),
            )
        with pytest.raises(MarketDataNotFound):
            await control.create_backfills(
                "SOLUSDT",
                request.model_copy(update={"symbol": "SOLUSDT"}),
            )

    asyncio.run(scenario())


def test_ui_inclusive_days_map_to_a_complete_utc_day_archive_plan() -> None:
    async def scenario() -> None:
        repository = configured_repository()
        control = service(repository)
        ui_start = datetime(2026, 7, 1, tzinfo=UTC)
        ui_inclusive_end_as_half_open = datetime(2026, 7, 21, tzinfo=UTC)

        await control.create_backfills(
            "BTCUSDT",
            BackfillRequest(
                symbol="BTCUSDT",
                data_types=(DataType.KLINE_1M,),
                start=ui_start,
                end=ui_inclusive_end_as_half_open,
            ),
        )

        planned = tuple(repository.planned.values())
        assert planned
        assert min(item.start for item in planned) == ui_start
        assert max(item.end for item in planned) == ui_inclusive_end_as_half_open
        assert all(
            item.start.tzinfo is UTC
            and item.end.tzinfo is UTC
            and item.start.time().isoformat() == "00:00:00"
            and item.end.time().isoformat() == "00:00:00"
            for item in planned
        )

    asyncio.run(scenario())


def test_service_keeps_profile_and_eligibility_evidence_symbol_specific() -> None:
    async def scenario() -> None:
        repository = configured_repository()
        control = service(repository)

        btc_profile = await control.get_profile("BTCUSDT")
        pepe_profile = await control.get_profile("PEPEUSDT")
        btc = await control.get_eligibility("BTCUSDT")
        pepe = await control.get_eligibility("PEPEUSDT")

        assert btc_profile.realized_volatility == 1.0
        assert pepe_profile.realized_volatility == 9.0
        assert "unrepaired_gap" not in btc.reason_codes
        assert "unrepaired_gap" in pepe.reason_codes
        assert btc.symbol == "BTCUSDT"
        assert pepe.symbol == "PEPEUSDT"

    asyncio.run(scenario())


def test_service_requires_gapless_current_coverage_for_each_archive_dataset() -> None:
    async def scenario() -> None:
        repository = configured_repository()
        repository.summaries["BTCUSDT"] = SimpleNamespace(
            approved_data_types=("kline_1m", "mark_price", "funding"),
            archive_intervals={
                "kline_1m": ((START, END),),
                "mark_price": ((START, START + timedelta(days=10)),
                               (START + timedelta(days=11), END)),
                "funding": ((START, START + timedelta(days=15)),
                            (START + timedelta(days=15), END)),
            },
            metadata_verified=True,
            open_gap_count=0,
            job_statuses=("succeeded",),
        )
        control = service(repository)

        view = await control.get_symbol("BTCUSDT")
        eligibility = await control.get_eligibility("BTCUSDT")

        assert view.data_status is not SymbolDataStatus.DATA_READY
        assert (
            EligibilityReasonCode.INSUFFICIENT_COVERAGE
            in eligibility.reason_codes
        )

    asyncio.run(scenario())


def test_eligibility_requires_every_expected_stream_connected_and_fresh() -> None:
    async def decision_for(streams: tuple[StreamState, ...]):
        repository = configured_repository()
        repository.summaries["BTCUSDT"] = SimpleNamespace(
            approved_data_types=("kline_1m", "mark_price", "funding"),
            archive_intervals={
                data_type: ((START, END),)
                for data_type in ("kline_1m", "mark_price", "funding")
            },
            metadata_verified=True,
            open_gap_count=0,
            job_statuses=("succeeded",),
        )
        repository.streams["BTCUSDT"] = streams
        return await service(repository).get_eligibility("BTCUSDT")

    async def scenario() -> None:
        expected = streams_for_symbols(("BTCUSDT",))
        healthy = tuple(
            StreamState(
                "BTCUSDT",
                stream.name,
                NOW - timedelta(seconds=1),
                "connected",
                {"source_mode": "direct"},
                NOW,
            )
            for stream in expected
        )

        missing = await decision_for(healthy[:-1])
        stale = await decision_for(
            (*healthy[:-1], replace(healthy[-1], last_event_at=NOW - timedelta(seconds=121)))
        )
        degraded = await decision_for(
            (*healthy[:-1], replace(healthy[-1], status="degraded"))
        )

        assert EligibilityReasonCode.STALE_LIVE_DATA in missing.reason_codes
        assert EligibilityReasonCode.SOURCE_DEGRADED in missing.reason_codes
        assert EligibilityReasonCode.STALE_LIVE_DATA in stale.reason_codes
        assert EligibilityReasonCode.SOURCE_DEGRADED in degraded.reason_codes

    asyncio.run(scenario())


def test_service_health_uses_persisted_worker_and_redacted_stream_details() -> None:
    async def scenario() -> None:
        repository = configured_repository()
        repository.streams["BTCUSDT"] = tuple(
            StreamState(
                "BTCUSDT",
                stream.name,
                NOW - timedelta(seconds=1),
                "connected",
                {
                    "source_mode": "proxy",
                    "proxy_url": "http://user:secret@localhost:17891/?token=secret",
                },
                NOW,
            )
            for stream in streams_for_symbols(("BTCUSDT",))
        )
        repository.symbols["PEPEUSDT"] = replace(
            repository.symbols["PEPEUSDT"], enabled=False
        )
        repository.streams["PEPEUSDT"] = (
            StreamState(
                "PEPEUSDT",
                "pepeusdt@kline_1m",
                NOW - timedelta(minutes=10),
                "disconnected",
                {"source_mode": "direct"},
                NOW,
            ),
        )
        repository.heartbeat = WorkerHeartbeat("market-worker", "running", NOW, {})

        health = await service(repository).market_data_health()

        assert health.source_mode.value == "proxy"
        assert health.worker_heartbeat_at == NOW
        assert health.archive_healthy is True
        assert health.rest_healthy is False
        assert "secret" not in health.model_dump_json()

    asyncio.run(scenario())


def test_operations_health_checks_all_expected_streams_and_worker_freshness() -> None:
    async def scenario() -> None:
        repository = Repository()
        for number in range(26):
            symbol = f"S{number:03d}USDT"
            repository.symbols[symbol] = SymbolState(
                symbol, True, START, END, False, NOW, NOW
            )
            repository.streams[symbol] = tuple(
                StreamState(
                    symbol,
                    stream.name,
                    NOW - timedelta(seconds=1),
                    "connected",
                    {"source_mode": "direct"},
                    NOW,
                )
                for stream in streams_for_symbols((symbol,))
            )
        last_symbol = "S025USDT"
        repository.streams[last_symbol] = repository.streams[last_symbol][:-1]
        repository.heartbeat = WorkerHeartbeat("market-worker", "running", NOW, {})

        incomplete = await service(repository).market_data_health()

        assert repository.health_aggregate_calls == 1
        assert len(incomplete.streams) == 103
        assert incomplete.source_mode.value == "degraded"
        assert incomplete.archive_healthy is True

        for heartbeat in (
            None,
            WorkerHeartbeat("market-worker", "stopped", NOW, {}),
            WorkerHeartbeat(
                "market-worker", "running", NOW - timedelta(seconds=121), {}
            ),
        ):
            repository.heartbeat = heartbeat
            unhealthy = await service(repository).market_data_health()
            assert unhealthy.source_mode.value == "degraded"
            assert unhealthy.archive_healthy is False

    asyncio.run(scenario())
