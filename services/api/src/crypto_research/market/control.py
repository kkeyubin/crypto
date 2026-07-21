from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession

from crypto_research.config import Settings
from crypto_research.contracts.data import (
    AddSymbolRequest,
    BackfillRequest,
    DataGapStatus,
    DataGapView,
    DataPartitionStatus,
    DataPartitionView,
    EligibilityView,
    IngestionJobStatus,
    IngestionJobView,
    MarketDataHealthView,
    MetadataStatus,
    SourceMode,
    StreamStateView,
    StreamStatus,
    SymbolDataStatus,
    SymbolProfileView,
    SymbolView,
)
from crypto_research.contracts.manifest import DataType
from crypto_research.db.repositories import (
    ActiveStreamHealth,
    AddSymbolCommand,
    BackfillCommand,
    DataGap,
    DataPartition,
    IngestionJob,
    MutationIdentityConflict,
    SqlAlchemyDataStateRepository,
    StreamState,
    SymbolOperationalSummary,
    SymbolState,
    WorkerHeartbeat,
)
from crypto_research.market.backfill import BackfillObject
from crypto_research.market.backfill_planning import ArchiveBackfillPlanner
from crypto_research.market.binance.archive_paths import DatasetKind, plan_archives
from crypto_research.market.binance.streams import streams_for_symbols
from crypto_research.market.catalog import SecureDuckDBCatalog, SqlAlchemyCatalogRepository
from crypto_research.market.eligibility import (
    EligibilityContext,
    SymbolEligibilityPolicy,
    evaluate_eligibility,
)
from crypto_research.market.live_catalog import (
    SecureLiveDuckDBCatalog,
    SqlAlchemyLiveCatalogRepository,
)
from crypto_research.market.profile import CatalogProfileService, SymbolProfile

_SYMBOL = re.compile(r"[A-Z0-9]{3,32}")
_REQUIRED_ARCHIVE_TYPES = frozenset(
    {DataType.KLINE_1M.value, DataType.MARK_PRICE.value, DataType.FUNDING.value}
)
_HISTORICAL_DATA_TYPES = frozenset(
    {DataType.KLINE_1M, DataType.MARK_PRICE, DataType.FUNDING, DataType.AGG_TRADE}
)
_ARCHIVE_DATASET = {
    DataType.KLINE_1M: DatasetKind.KLINES,
    DataType.MARK_PRICE: DatasetKind.MARK_PRICE_KLINES,
    DataType.FUNDING: DatasetKind.FUNDING_RATE,
    DataType.AGG_TRADE: DatasetKind.AGG_TRADES,
}


class MarketDataNotFound(LookupError):
    """A requested configured market-data resource does not exist."""


class MarketDataConflict(ValueError):
    """A valid request conflicts with durable market-data identity or state."""


class MarketDataValidationError(ValueError):
    """A domain limit is stricter than the reusable wire contract."""


class MarketDataRepository(Protocol):
    async def list_symbols(
        self, *, limit: int, offset: int
    ) -> tuple[SymbolState, ...]: ...

    async def get_symbol(self, symbol: str) -> SymbolState | None: ...

    async def list_active_symbols(self) -> tuple[SymbolState, ...]: ...

    async def add_symbol(self, command: AddSymbolCommand) -> SymbolState: ...

    async def set_symbol_enabled(self, symbol: str, enabled: bool) -> SymbolState: ...

    async def create_backfill(self, command: BackfillCommand) -> IngestionJob: ...

    async def get_backfill(self, job_id: str) -> IngestionJob | None: ...

    async def list_partitions(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataPartition, ...]: ...

    async def list_gaps(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataGap, ...]: ...

    async def list_stream_states(
        self,
        symbol: str | None = None,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[StreamState, ...]: ...

    async def get_symbol_summary(self, symbol: str) -> SymbolOperationalSummary: ...

    async def list_symbol_summaries(
        self, symbols: tuple[SymbolState, ...]
    ) -> dict[str, SymbolOperationalSummary]: ...

    async def list_active_stream_states(self) -> tuple[StreamState, ...]: ...

    async def get_active_stream_health(
        self, *, checked_at: datetime, stale_after: timedelta
    ) -> ActiveStreamHealth: ...

    async def get_worker_heartbeat(self, worker_id: str) -> WorkerHeartbeat | None: ...

    async def count_failed_jobs(self) -> int: ...

    async def plan(self, work: BackfillObject) -> BackfillObject: ...


class SymbolProfileProvider(Protocol):
    async def compute(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        calculated_at: datetime,
    ) -> SymbolProfile: ...


class MarketDataControl(Protocol):
    async def list_symbols(self, *, limit: int, offset: int) -> tuple[SymbolView, ...]: ...

    async def add_symbol(self, request: AddSymbolRequest) -> SymbolView: ...

    async def get_symbol(self, symbol: str) -> SymbolView: ...

    async def disable_symbol(self, symbol: str) -> SymbolView: ...

    async def create_backfills(
        self, symbol: str, request: BackfillRequest
    ) -> tuple[IngestionJobView, ...]: ...

    async def get_backfill(self, job_id: UUID) -> IngestionJobView: ...

    async def list_partitions(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataPartitionView, ...]: ...

    async def list_gaps(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataGapView, ...]: ...

    async def get_profile(self, symbol: str) -> SymbolProfileView: ...

    async def get_eligibility(self, symbol: str) -> EligibilityView: ...

    async def list_streams(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[StreamStateView, ...]: ...

    async def market_data_health(self) -> MarketDataHealthView: ...


class MarketDataControlService:
    """Own public market-data rules while repositories own transaction state."""

    def __init__(
        self,
        repository: MarketDataRepository,
        profiles: SymbolProfileProvider,
        settings: Settings,
        *,
        clock=lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._profiles = profiles
        self._settings = settings
        self._clock = clock

    async def list_symbols(self, *, limit: int, offset: int) -> tuple[SymbolView, ...]:
        states = await self._repository.list_symbols(limit=limit, offset=offset)
        summaries = await self._repository.list_symbol_summaries(states)
        return tuple(
            [
                await self._symbol_view(state, summaries[state.symbol])
                for state in states
            ]
        )

    async def add_symbol(self, request: AddSymbolRequest) -> SymbolView:
        self._validate_history_range(request.history_start, request.history_end)
        existing = await self._repository.get_symbol(request.symbol)
        if existing is not None:
            identity = (
                existing.history_start,
                existing.history_end,
                existing.include_agg_trades,
            )
            requested = (
                request.history_start,
                request.history_end,
                request.include_agg_trades,
            )
            if identity != requested:
                raise MarketDataConflict("symbol configuration conflicts with existing history")
            if not existing.enabled:
                existing = await self._repository.set_symbol_enabled(request.symbol, True)
            return await self._symbol_view(existing)
        try:
            created = await self._repository.add_symbol(
                AddSymbolCommand(
                    request.symbol,
                    request.history_start,
                    request.history_end,
                    request.include_agg_trades,
                )
            )
        except MutationIdentityConflict as error:
            raise MarketDataConflict(str(error)) from error
        return await self._symbol_view(created)

    async def get_symbol(self, symbol: str) -> SymbolView:
        state = await self._required_symbol(symbol)
        return await self._symbol_view(state)

    async def disable_symbol(self, symbol: str) -> SymbolView:
        await self._required_symbol(symbol)
        disabled = await self._repository.set_symbol_enabled(symbol, False)
        return await self._symbol_view(disabled)

    async def create_backfills(
        self, symbol: str, request: BackfillRequest
    ) -> tuple[IngestionJobView, ...]:
        symbol = _normalized_symbol(symbol)
        if symbol != request.symbol:
            raise MarketDataConflict("path symbol conflicts with request symbol")
        configured = await self._required_symbol(symbol)
        if not configured.enabled:
            raise MarketDataConflict("disabled symbols cannot accept new backfills")
        self._validate_history_range(request.start, request.end)
        unsupported = set(request.data_types) - _HISTORICAL_DATA_TYPES
        if unsupported:
            raise MarketDataValidationError(
                "historical backfills accept only official archive datasets"
            )
        if DataType.AGG_TRADE in request.data_types and not configured.include_agg_trades:
            raise MarketDataConflict(
                "aggregate-trade history requires symbol-level explicit opt-in"
            )
        jobs: list[IngestionJobView] = []
        for data_type in sorted(set(request.data_types), key=lambda item: item.value):
            identity = "|".join(
                (
                    symbol,
                    data_type.value,
                    request.start.isoformat(),
                    request.end.isoformat(),
                )
            )
            job_id = str(uuid5(NAMESPACE_URL, f"crypto-research:backfill:{identity}"))
            existing = await self._repository.get_backfill(job_id)
            if existing is not None and (
                existing.symbol != symbol
                or existing.dataset != data_type.value
                or existing.requested_start != request.start
                or existing.requested_end != request.end
            ):
                raise MarketDataConflict("backfill identity conflicts with durable state")
            try:
                stored = existing or await self._repository.create_backfill(
                    BackfillCommand(
                        id=job_id,
                        symbol=symbol,
                        dataset=data_type.value,
                        requested_start=request.start,
                        requested_end=request.end,
                    )
                )
                archives = plan_archives(
                    _ARCHIVE_DATASET[data_type],
                    symbol,
                    request.start,
                    request.end,
                    as_of=self._clock(),
                )
            except MutationIdentityConflict as error:
                raise MarketDataConflict(str(error)) from error
            except ValueError as error:
                raise MarketDataValidationError(
                    "backfill range must contain complete closed UTC days"
                ) from error
            try:
                await ArchiveBackfillPlanner(self._repository).plan(stored.id, archives)
            except MutationIdentityConflict as error:
                raise MarketDataConflict(str(error)) from error
            jobs.append(_job_view(stored, fallback_now=self._clock()))
        return tuple(jobs)

    async def get_backfill(self, job_id: UUID) -> IngestionJobView:
        job = await self._repository.get_backfill(str(job_id))
        if job is None:
            raise MarketDataNotFound("backfill does not exist")
        return _job_view(job, fallback_now=self._clock())

    async def list_partitions(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataPartitionView, ...]:
        symbol = (await self._required_symbol(symbol)).symbol
        values = await self._repository.list_partitions(
            symbol, limit=limit, offset=offset
        )
        return tuple(_partition_view(item, fallback_now=self._clock()) for item in values)

    async def list_gaps(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[DataGapView, ...]:
        symbol = (await self._required_symbol(symbol)).symbol
        values = await self._repository.list_gaps(symbol, limit=limit, offset=offset)
        return tuple(_gap_view(item, fallback_now=self._clock()) for item in values)

    async def get_profile(self, symbol: str) -> SymbolProfileView:
        configured = await self._required_symbol(symbol)
        profile = await self._compute_profile(configured)
        if profile is None:
            raise MarketDataNotFound("approved profile evidence is not available")
        return _profile_view(profile)

    async def get_eligibility(self, symbol: str) -> EligibilityView:
        configured = await self._required_symbol(symbol)
        summary = await self._repository.get_symbol_summary(configured.symbol)
        profile = await self._compute_profile(configured)
        now = self._clock()
        streams = await self._repository.list_stream_states(
            configured.symbol, limit=100, offset=0
        )
        live_last_event, required_source_degraded = _live_gate(
            configured.symbol,
            streams,
        )
        decision = evaluate_eligibility(
            EligibilityContext(
                symbol=configured.symbol,
                metadata_verified=summary.metadata_verified,
                history_start=_required_time(configured.history_start, "history_start"),
                history_end=_required_time(configured.history_end, "history_end"),
                coverage_fraction=(
                    profile.realized_volatility.coverage_fraction
                    if profile and _archive_coverage_complete(configured, summary)
                    else 0.0
                ),
                median_hourly_volume=(
                    profile.median_hourly_volume.value if profile else 0.0
                ),
                live_last_event_at=live_last_event,
                unrepaired_gap_count=summary.open_gap_count,
                data_ready=_data_status(configured, summary)
                is SymbolDataStatus.DATA_READY,
                required_source_degraded=required_source_degraded,
            ),
            SymbolEligibilityPolicy(
                symbol=configured.symbol,
                minimum_history=timedelta(days=30),
                minimum_coverage_fraction=0.99,
                minimum_median_hourly_volume=0,
                live_stale_after=timedelta(
                    seconds=self._settings.live_stale_after_seconds
                ),
            ),
            now=now,
        )
        return EligibilityView(
            symbol=decision.symbol,
            eligible=decision.eligible,
            reason_codes=decision.reason_codes,
            evaluated_at=decision.evaluated_at,
        )

    async def list_streams(
        self, symbol: str, *, limit: int, offset: int
    ) -> tuple[StreamStateView, ...]:
        symbol = (await self._required_symbol(symbol)).symbol
        values = await self._repository.list_stream_states(
            symbol, limit=limit, offset=offset
        )
        return tuple(_stream_view(item, fallback_now=self._clock()) for item in values)

    async def market_data_health(self) -> MarketDataHealthView:
        streams = await self._repository.list_active_stream_states()
        heartbeat = await self._repository.get_worker_heartbeat(
            self._settings.market_worker_id
        )
        failed_jobs = await self._repository.count_failed_jobs()
        checked_at = self._clock()
        stale_after = timedelta(seconds=self._settings.live_stale_after_seconds)
        stream_health = await self._repository.get_active_stream_health(
            checked_at=checked_at, stale_after=stale_after
        )
        worker_healthy = (
            heartbeat is not None
            and heartbeat.status == "running"
            and checked_at - stale_after <= heartbeat.heartbeat_at <= checked_at
        )
        live_healthy = (
            stream_health.expected_count > 0
            and stream_health.observed_count == stream_health.expected_count
            and stream_health.healthy_count == stream_health.expected_count
        )
        return MarketDataHealthView(
            source_mode=(
                _source_mode(streams)
                if worker_healthy and live_healthy
                else SourceMode.DEGRADED
            ),
            archive_healthy=failed_jobs == 0 and worker_healthy,
            # REST capability health is not durable yet, so the public API fails closed.
            rest_healthy=False,
            worker_heartbeat_at=None if heartbeat is None else heartbeat.heartbeat_at,
            streams=tuple(
                _stream_view(item, fallback_now=self._clock()) for item in streams
            ),
            checked_at=checked_at,
        )

    async def _required_symbol(self, symbol: str) -> SymbolState:
        normalized = _normalized_symbol(symbol)
        state = await self._repository.get_symbol(normalized)
        if state is None:
            raise MarketDataNotFound("symbol is not configured")
        return state

    async def _symbol_view(
        self,
        state: SymbolState,
        summary: SymbolOperationalSummary | None = None,
    ) -> SymbolView:
        summary = summary or await self._repository.get_symbol_summary(state.symbol)
        now = self._clock()
        return SymbolView(
            symbol=state.symbol,
            enabled=state.enabled,
            history_start=_required_time(state.history_start, "history_start"),
            history_end=_required_time(state.history_end, "history_end"),
            include_agg_trades=state.include_agg_trades,
            data_status=_data_status(state, summary),
            metadata_status=(
                MetadataStatus.PROFILE_BUILDING
                if summary.metadata_verified
                else MetadataStatus.METADATA_UNVERIFIED
            ),
            created_at=state.created_at or now,
            updated_at=state.updated_at or now,
        )

    async def _compute_profile(self, state: SymbolState) -> SymbolProfile | None:
        profile = await self._profiles.compute(
            state.symbol,
            _required_time(state.history_start, "history_start"),
            _required_time(state.history_end, "history_end"),
            calculated_at=self._clock(),
        )
        if profile.realized_volatility.sample_count <= 0:
            return None
        return profile

    def _validate_history_range(self, start: datetime, end: datetime) -> None:
        maximum = timedelta(days=self._settings.history_max_days)
        if end - start > maximum:
            raise MarketDataValidationError(
                f"history range cannot exceed {self._settings.history_max_days} days"
            )


class SqlAlchemyProfileProvider:
    """Build a profile only from checksum-verified approved archive/live catalogs."""

    def __init__(self, session: AsyncSession, data_root: Path) -> None:
        self._session = session
        self._data_root = data_root

    async def compute(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        calculated_at: datetime,
    ) -> SymbolProfile:
        archive = SecureDuckDBCatalog(
            self._data_root, SqlAlchemyCatalogRepository(self._session)
        )
        live = SecureLiveDuckDBCatalog(
            self._data_root, SqlAlchemyLiveCatalogRepository(self._session)
        )
        reader = _ProfileCatalogReader(archive, live)
        return await CatalogProfileService(reader).compute(
            symbol, start, end, calculated_at=calculated_at
        )


class _ProfileCatalogReader:
    def __init__(
        self, archive: SecureDuckDBCatalog, live: SecureLiveDuckDBCatalog
    ) -> None:
        self._archive = archive
        self._live = live

    async def read(
        self, symbol: str, data_type: DataType, start: datetime, end: datetime
    ) -> tuple[Mapping[str, object], ...]:
        if data_type is not DataType.BEST_BID_ASK:
            return await self._archive.read(symbol, data_type, start, end)
        rows = await self._live.read(
            symbol,
            "book_ticker",
            _epoch_milliseconds(start),
            _epoch_milliseconds(end),
        )
        return tuple(
            {
                **row,
                "event_time": row["transact_time"],
                "best_bid": row["bid_price"],
                "best_ask": row["ask_price"],
            }
            for row in rows
        )


def build_market_data_control(
    session: AsyncSession, settings: Settings
) -> MarketDataControlService:
    return MarketDataControlService(
        SqlAlchemyDataStateRepository(session),
        SqlAlchemyProfileProvider(session, settings.data_root),
        settings,
    )


def _data_status(
    state: SymbolState, summary: SymbolOperationalSummary
) -> SymbolDataStatus:
    if not state.enabled:
        return SymbolDataStatus.DISABLED
    statuses = set(summary.job_statuses)
    if statuses & {"queued", "running"}:
        return SymbolDataStatus.BACKFILLING
    if _archive_coverage_complete(state, summary):
        if summary.open_gap_count or "failed" in statuses:
            return SymbolDataStatus.DEGRADED
        return SymbolDataStatus.DATA_READY
    approved = set(summary.approved_data_types)
    if "failed" in statuses and not approved:
        return SymbolDataStatus.FAILED
    if summary.open_gap_count or "failed" in statuses:
        return SymbolDataStatus.DEGRADED
    return SymbolDataStatus.REQUESTED


def _archive_coverage_complete(
    state: SymbolState, summary: SymbolOperationalSummary
) -> bool:
    start = _required_time(state.history_start, "history_start")
    end = _required_time(state.history_end, "history_end")
    by_dataset: dict[str, list[tuple[datetime, datetime]]] = {
        data_type: [] for data_type in _REQUIRED_ARCHIVE_TYPES
    }
    values = summary.archive_intervals
    if isinstance(values, Mapping):
        for dataset, intervals in values.items():
            if dataset in by_dataset:
                by_dataset[dataset].extend(intervals)
    else:
        for interval in values:
            if interval.dataset in by_dataset:
                by_dataset[interval.dataset].append((interval.start, interval.end))
    return all(
        _intervals_cover(intervals, start, end)
        for intervals in by_dataset.values()
    )


def _intervals_cover(
    intervals: list[tuple[datetime, datetime]], start: datetime, end: datetime
) -> bool:
    cursor = start
    for interval_start, interval_end in sorted(intervals):
        if interval_start >= interval_end or interval_end <= cursor:
            continue
        if interval_start > cursor:
            return False
        cursor = max(cursor, interval_end)
        if cursor >= end:
            return True
    return False


def _job_view(job: IngestionJob, *, fallback_now: datetime) -> IngestionJobView:
    return IngestionJobView(
        job_id=UUID(job.id),
        symbol=job.symbol,
        data_type=DataType(job.dataset),
        status=IngestionJobStatus(job.status),
        requested_start=_required_time(job.requested_start, "requested_start"),
        requested_end=_required_time(job.requested_end, "requested_end"),
        created_at=job.created_at or fallback_now,
        updated_at=job.updated_at or fallback_now,
    )


def _partition_view(
    partition: DataPartition, *, fallback_now: datetime
) -> DataPartitionView:
    return DataPartitionView(
        partition_id=UUID(partition.id),
        symbol=partition.symbol,
        data_type=DataType(partition.dataset),
        start=_required_time(partition.start_at, "partition start"),
        end=_required_time(partition.end_at, "partition end"),
        parquet_path=partition.parquet_path,
        checksum=partition.checksum_sha256,
        row_count=_required_positive(partition.row_count, "partition row count"),
        version=partition.version,
        status=DataPartitionStatus(partition.approval_status),
        created_at=partition.created_at or fallback_now,
        approved_at=partition.approved_at,
    )


def _gap_view(gap: DataGap, *, fallback_now: datetime) -> DataGapView:
    return DataGapView(
        gap_id=UUID(gap.id),
        symbol=gap.symbol,
        data_type=DataType(gap.dataset),
        start=gap.start_at,
        end=gap.end_at,
        reason=gap.reason,
        status=DataGapStatus(gap.status),
        opened_at=gap.opened_at or fallback_now,
        repaired_at=gap.repaired_at,
    )


def _profile_view(profile: SymbolProfile) -> SymbolProfileView:
    return SymbolProfileView(
        symbol=profile.symbol,
        calculated_at=profile.calculated_at,
        coverage_start=profile.coverage_start,
        coverage_end=profile.coverage_end,
        sample_count=profile.realized_volatility.sample_count,
        coverage_fraction=profile.realized_volatility.coverage_fraction,
        realized_volatility=profile.realized_volatility.value,
        jump_frequency=profile.jump_frequency.value,
        median_spread_bps=profile.median_spread_bps.value,
        median_hourly_volume=profile.median_hourly_volume.value,
        funding_rate_mean=profile.funding_rate_mean.value,
    )


def _stream_view(stream: StreamState, *, fallback_now: datetime) -> StreamStateView:
    return StreamStateView(
        symbol=stream.symbol,
        stream_name=stream.stream_name,
        status=StreamStatus(stream.status),
        last_event_at=stream.last_event_at,
        updated_at=stream.updated_at or fallback_now,
    )


def _source_mode(streams: tuple[StreamState, ...]) -> SourceMode:
    if any(item.status in {"degraded", "disconnected"} for item in streams):
        return SourceMode.DEGRADED
    modes = {
        item.details.get("source_mode")
        for item in streams
        if isinstance(item.details, dict)
    }
    if "proxy" in modes:
        return SourceMode.PROXY
    if "direct" in modes:
        return SourceMode.DIRECT
    return SourceMode.DEGRADED


def _live_gate(
    symbol: str, streams: tuple[StreamState, ...]
) -> tuple[datetime | None, bool]:
    expected = {stream.name for stream in streams_for_symbols((symbol,))}
    by_name = {
        stream.stream_name: stream
        for stream in streams
        if stream.symbol == symbol and stream.stream_name in expected
    }
    if set(by_name) != expected:
        return None, True
    ordered = tuple(by_name[name] for name in sorted(expected))
    last_events = tuple(stream.last_event_at for stream in ordered)
    return (
        None if any(value is None for value in last_events) else min(last_events),
        any(stream.status != "connected" for stream in ordered),
    )


def _normalized_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if _SYMBOL.fullmatch(normalized) is None:
        raise MarketDataValidationError("symbol has invalid syntax")
    return normalized


def _required_time(value: datetime | None, field_name: str) -> datetime:
    if value is None:
        raise ValueError(f"persisted {field_name} is missing")
    return value


def _required_positive(value: int | None, field_name: str) -> int:
    if value is None or value <= 0:
        raise ValueError(f"persisted {field_name} is invalid")
    return value


def _epoch_milliseconds(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000
