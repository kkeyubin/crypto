import asyncio
from datetime import UTC, datetime, timedelta, timezone

import pytest

from crypto_research.db.models import (
    AuditEventRow,
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


class FakeAsyncSession:
    def __init__(self) -> None:
        self.rows: list[object] = []

    async def get(self, model: type[object], identity: object) -> object | None:
        for row in self.rows:
            if type(row) is not model:
                continue
            if isinstance(row, SymbolRow) and row.symbol == identity:
                return row
            if (
                isinstance(row, (DataGapRow, DataPartitionRow, IngestionJobRow))
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
                dataset="klines_1m",
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
                dataset="klines_1m",
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
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]
        opened = await repository.record_gap(
            GapRecord(
                id="00000000-0000-0000-0000-000000000021",
                symbol="BTCUSDT",
                dataset="klines_1m",
                start_at=datetime(2026, 7, 20, tzinfo=UTC),
                end_at=datetime(2026, 7, 20, 0, 1, tzinfo=UTC),
                reason="disconnect",
            )
        )
        repaired = await repository.repair_gap(opened.id, {"method": "archive"})

        assert opened.status == "open"
        assert repaired.status == "repaired"
        assert repaired.repaired_at is not None
        assert repaired.repair_details == {"method": "archive"}

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
