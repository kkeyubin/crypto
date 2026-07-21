import asyncio
from datetime import UTC, datetime, timedelta, timezone

import pytest

from crypto_research.contracts.manifest import DataType
from crypto_research.db.models import (
    AuditEventRow,
    BackfillObjectRow,
    DataGapRow,
    DataPartitionRow,
    IngestionJobRow,
    StreamStateRow,
    SymbolRow,
)
from crypto_research.db.repositories import (
    AddSymbolCommand,
    BackfillCommand,
    GapRecord,
    PartitionCandidate,
    SqlAlchemyDataStateRepository,
    StreamState,
)
from crypto_research.market.backfill import BackfillObject, BackfillState
from crypto_research.market.gaps import ApprovedCoverage, TimeRange


class FakeAsyncSession:
    def __init__(self) -> None:
        self.rows: list[object] = []

    async def get(
        self, model: type[object], identity: object, **_kwargs: object
    ) -> object | None:
        for row in self.rows:
            if type(row) is not model:
                continue
            if isinstance(row, SymbolRow) and row.symbol == identity:
                return row
            if (
                isinstance(
                    row, (BackfillObjectRow, DataGapRow, DataPartitionRow, IngestionJobRow)
                )
                and row.id == identity
            ):
                return row
            if isinstance(row, StreamStateRow) and (row.symbol, row.stream_name) == identity:
                return row
        return None

    def add(self, row: object) -> None:
        self.rows.append(row)

    async def flush(self) -> None:
        return None


class FakeCoverageResolver:
    def __init__(self, coverage: tuple[ApprovedCoverage, ...]) -> None:
        self.coverage = {item.partition_id: item for item in coverage}

    async def resolve(
        self, partition_ids: tuple[str, ...]
    ) -> tuple[ApprovedCoverage, ...]:
        return tuple(
            self.coverage[partition_id]
            for partition_id in partition_ids
            if partition_id in self.coverage
        )


def test_add_disable_symbol_is_idempotent_and_audited() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        command = AddSymbolCommand(
            symbol="btcusdt",
            history_start="2026-01-01T00:00:00Z",
            history_end="2026-01-02T00:00:00Z",
        )

        first = await repository.add_symbol(command)
        second = await repository.add_symbol(command)
        disabled = await repository.disable_symbol("BTCUSDT")
        disabled_again = await repository.disable_symbol("btcusdt")

        assert first == second
        assert disabled.enabled is False
        assert disabled_again == disabled
        assert [row.symbol for row in session.rows if isinstance(row, SymbolRow)] == ["BTCUSDT"]
        assert len([row for row in session.rows if isinstance(row, AuditEventRow)]) == 2

    asyncio.run(scenario())


def test_symbols_and_backfills_are_independent() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        await repository.add_symbol(
            AddSymbolCommand("BTCUSDT", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
        )
        await repository.add_symbol(
            AddSymbolCommand("pepeusdt", "2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z")
        )

        btc = await repository.create_backfill(
            BackfillCommand(
                id="00000000-0000-0000-0000-000000000001",
                symbol="BTCUSDT",
                dataset="kline_1m",
            )
        )
        pepe = await repository.create_backfill(
            BackfillCommand(
                id="00000000-0000-0000-0000-000000000002",
                symbol="PEPEUSDT",
                dataset="funding_rate",
            )
        )

        assert btc.symbol == "BTCUSDT"
        assert pepe.symbol == "PEPEUSDT"
        assert btc.id != pepe.id

    asyncio.run(scenario())


def test_job_transitions_accept_only_legal_next_states() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        await repository.add_symbol(
            AddSymbolCommand("BTCUSDT", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
        )
        job = await repository.create_backfill(
            BackfillCommand(
                id="00000000-0000-0000-0000-000000000003",
                symbol="BTCUSDT",
                dataset="kline_1m",
            )
        )

        running = await repository.transition_job(job.id, "running")
        succeeded = await repository.transition_job(job.id, "succeeded")

        assert running.status == "running"
        assert succeeded.status == "succeeded"
        with pytest.raises(ValueError, match="illegal"):
            await repository.transition_job(job.id, "running")

    asyncio.run(scenario())


def test_approved_partition_is_idempotent_but_refuses_version_replacement() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        candidate = PartitionCandidate(
            id="00000000-0000-0000-0000-000000000011",
            symbol="BTCUSDT",
            dataset="klines_1m",
            partition_date="2026-07-20",
            version=1,
            checksum_sha256="a" * 64,
            parquet_path="normalized/btc.parquet",
        )

        first = await repository.approve_partition(candidate)
        second = await repository.approve_partition(candidate)
        assert first == second

        with pytest.raises(ValueError, match="immutable"):
            await repository.approve_partition(
                PartitionCandidate(**{**candidate.__dict__, "checksum_sha256": "b" * 64})
            )

    asyncio.run(scenario())


def test_gaps_keep_open_and_repair_history() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        partial_coverage = ApprovedCoverage(
            "BTCUSDT",
            DataType.KLINE_1M,
            TimeRange(
                datetime(2026, 7, 20, tzinfo=UTC),
                datetime(2026, 7, 20, 0, 0, 30, tzinfo=UTC),
            ),
            "partial-partition",
        )
        full_coverage = ApprovedCoverage(
            "BTCUSDT",
            DataType.KLINE_1M,
            TimeRange(
                datetime(2026, 7, 20, tzinfo=UTC),
                datetime(2026, 7, 20, 0, 1, tzinfo=UTC),
            ),
            "full-partition",
        )
        repository = SqlAlchemyDataStateRepository(  # type: ignore[arg-type]
            session, FakeCoverageResolver((partial_coverage, full_coverage))
        )
        opened = await repository.record_gap(
            GapRecord(
                id="00000000-0000-0000-0000-000000000021",
                symbol="BTCUSDT",
                dataset="kline_1m",
                start_at=datetime(2026, 7, 20, tzinfo=UTC),
                end_at=datetime(2026, 7, 20, 0, 1, tzinfo=UTC),
                reason="disconnect",
            )
        )
        partial = await repository.reconcile_gap(
            opened.id,
            ("partial-partition",),
            datetime(2026, 7, 20, 1, tzinfo=UTC),
            "archive",
        )
        repaired = await repository.reconcile_gap(
            opened.id,
            ("full-partition",),
            datetime(2026, 7, 20, 2, tzinfo=UTC),
            "archive",
        )

        assert opened.status == "open"
        assert partial.status == "open"
        assert repaired.status == "repaired"
        assert repaired.repaired_at is not None
        assert repaired.repair_details is not None
        assert [item["result"] for item in repaired.repair_details["history"]] == [
            "partial",
            "repaired",
        ]

    asyncio.run(scenario())


def test_aggregate_trade_gap_persists_and_requires_id_repair_evidence() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        start = datetime(2026, 7, 20, tzinfo=UTC)
        end = datetime(2026, 7, 20, 0, 0, 1, tzinfo=UTC)
        time_only_coverage = ApprovedCoverage(
            "BTCUSDT",
            DataType.AGG_TRADE,
            TimeRange(start, end),
            "time-only",
        )
        id_coverage = ApprovedCoverage(
            "BTCUSDT",
            DataType.AGG_TRADE,
            TimeRange(start, end),
            "id-evidence",
            recovered_id_start=11,
            recovered_id_end=14,
        )
        repository = SqlAlchemyDataStateRepository(  # type: ignore[arg-type]
            session, FakeCoverageResolver((time_only_coverage, id_coverage))
        )
        opened = await repository.record_gap(
            GapRecord(
                id="00000000-0000-0000-0000-000000000022",
                symbol="BTCUSDT",
                dataset="agg_trade",
                start_at=start,
                end_at=end,
                reason="aggregate_trade_id_discontinuity",
                details={"missing_id_start": 11, "missing_id_end": 14},
            )
        )
        time_only = await repository.reconcile_gap(
            opened.id,
            ("time-only",),
            datetime(2026, 7, 20, 1, tzinfo=UTC),
            "archive",
        )
        repaired = await repository.reconcile_gap(
            opened.id,
            ("id-evidence",),
            datetime(2026, 7, 20, 2, tzinfo=UTC),
            "rest",
        )

        assert time_only.status == "open"
        assert repaired.status == "repaired"
        assert repaired.repair_details is not None
        assert repaired.repair_details["gap"] == {
            "missing_id_start": 11,
            "missing_id_end": 14,
        }

    asyncio.run(scenario())


def test_stream_heartbeat_upsert_preserves_one_row_per_stream() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        first_seen = datetime(2026, 7, 20, tzinfo=UTC)
        later_seen = datetime(2026, 7, 20, 0, 1, tzinfo=UTC)

        await repository.update_stream(StreamState("BTCUSDT", "kline", first_seen, "connected"))
        await repository.update_stream(StreamState("BTCUSDT", "kline", later_seen, "connected"))

        streams = [row for row in session.rows if isinstance(row, StreamStateRow)]
        assert len(streams) == 1
        assert streams[0].last_event_at == later_seen

    asyncio.run(scenario())


def test_backfill_object_lease_is_fenced_and_restart_resumes_durable_state() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        start = datetime(2026, 7, 20, tzinfo=UTC)
        row = BackfillObjectRow(
            id="00000000-0000-0000-0000-000000000041",
            job_id="00000000-0000-0000-0000-000000000042",
            source_url="https://data.binance.vision/day.zip",
            source_checksum="",
            start_at=start,
            end_at=start + timedelta(days=1),
            state="planned",
            attempt_count=0,
        )
        session.add(row)

        claimed = await repository.claim_backfill_object_row(
            row, "worker-a", start, timedelta(minutes=1)
        )
        downloading = await repository.advance_backfill_object(
            row.id,
            "worker-a",
            claimed.attempt_count,
            start,
            BackfillState.DOWNLOADING,
        )
        restarted = await repository.claim_backfill_object_row(
            row, "worker-b", start + timedelta(minutes=2), timedelta(minutes=1)
        )

        assert claimed.attempt_count == 1
        assert downloading.state is BackfillState.DOWNLOADING
        assert restarted.state is BackfillState.DOWNLOADING
        assert restarted.attempt_count == 2
        with pytest.raises(ValueError, match="lease"):
            await repository.renew_backfill_object(
                row.id,
                "worker-a",
                claimed.attempt_count,
                start + timedelta(minutes=2),
                timedelta(minutes=1),
            )

    asyncio.run(scenario())


def test_planning_backfill_object_is_idempotent_by_immutable_identity() -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        start = datetime(2026, 7, 20, tzinfo=UTC)
        work = BackfillObject(
            object_id="00000000-0000-0000-0000-000000000051",
            job_id="00000000-0000-0000-0000-000000000052",
            source_url="https://data.binance.vision/day.zip",
            start=start,
            end=start + timedelta(days=1),
        )

        first = await repository.plan_backfill_object(work)
        stored = next(
            item for item in session.rows if isinstance(item, BackfillObjectRow)
        )
        stored.source_checksum = "a" * 64
        stored.raw_path = "raw/a.zip"
        stored.state = BackfillState.CHECKSUM_VERIFIED.value
        second = await repository.plan_backfill_object(work)
        assert first.object_id == second.object_id
        assert second.source_checksum == "a" * 64
        assert len([item for item in session.rows if isinstance(item, BackfillObjectRow)]) == 1

        with pytest.raises(ValueError, match="immutable"):
            await repository.plan_backfill_object(
                BackfillObject(
                    **{**work.__dict__, "source_url": "https://example.invalid/replaced.zip"}
                )
            )

    asyncio.run(scenario())


def test_postgres_claim_statement_uses_skip_locked() -> None:
    class ScalarResult:
        def __init__(self, row: BackfillObjectRow) -> None:
            self.row = row

        def first(self) -> BackfillObjectRow:
            return self.row

    class Result:
        def __init__(self, row: BackfillObjectRow) -> None:
            self.row = row

        def scalars(self) -> ScalarResult:
            return ScalarResult(self.row)

    class ClaimSession(FakeAsyncSession):
        statements: list[object]

        def __init__(self) -> None:
            super().__init__()
            self.statements = []

        async def execute(self, statement: object) -> Result:
            self.statements.append(statement)
            return Result(next(row for row in self.rows if isinstance(row, BackfillObjectRow)))

    async def scenario() -> None:
        session = ClaimSession()
        start = datetime(2026, 7, 20, tzinfo=UTC)
        session.add(
            BackfillObjectRow(
                id="00000000-0000-0000-0000-000000000061",
                job_id="00000000-0000-0000-0000-000000000062",
                source_url="https://data.binance.vision/day.zip",
                source_checksum="",
                start_at=start,
                end_at=start + timedelta(days=1),
                state="planned",
                attempt_count=0,
            )
        )
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]

        claimed = await repository.claim("worker-a", start, timedelta(minutes=1))

        assert claimed is not None
        assert len(session.statements) == 2
        for_update = session.statements[0]._for_update_arg  # type: ignore[attr-defined]
        assert for_update.skip_locked is True

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "invalid_at",
    [
        datetime(2026, 7, 20),
        datetime(2026, 7, 20, tzinfo=timezone(timedelta(hours=8))),
    ],
    ids=["naive", "non-utc"],
)
def test_repository_rejects_non_utc_datetimes_at_every_command_boundary(
    invalid_at: datetime,
) -> None:
    async def scenario() -> None:
        session = FakeAsyncSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        await repository.add_symbol(
            AddSymbolCommand("BTCUSDT", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
        )

        operations = [
            lambda: repository.add_symbol(
                AddSymbolCommand("PEPEUSDT", invalid_at, datetime(2026, 1, 2, tzinfo=UTC))
            ),
            lambda: repository.create_backfill(
                BackfillCommand(
                    id="00000000-0000-0000-0000-000000000031",
                    symbol="BTCUSDT",
                    dataset="klines_1m",
                    requested_start=invalid_at,
                )
            ),
            lambda: repository.record_gap(
                GapRecord(
                    id="00000000-0000-0000-0000-000000000032",
                    symbol="BTCUSDT",
                    dataset="klines_1m",
                    start_at=invalid_at,
                    end_at=datetime(2026, 7, 20, 0, 1, tzinfo=UTC),
                    reason="disconnect",
                )
            ),
            lambda: repository.update_stream(
                StreamState("BTCUSDT", "kline", invalid_at, "connected")
            ),
        ]

        for operation in operations:
            with pytest.raises(ValueError, match="UTC-aware"):
                await operation()

    asyncio.run(scenario())
