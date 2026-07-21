import asyncio
from datetime import UTC, datetime

from crypto_research.db.models import IngestionJobRow
from crypto_research.db.repositories import SqlAlchemyDataStateRepository


class Result:
    @staticmethod
    def all():
        return []

    def scalars(self):
        return self

    @staticmethod
    def first():
        return None


class CapturingSession:
    def __init__(self) -> None:
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return Result()


def test_partition_api_repository_reads_only_approved_bounded_symbol_rows() -> None:
    async def scenario() -> None:
        session = CapturingSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]

        assert await repository.list_partitions("BTCUSDT", limit=20, offset=5) == ()

        assert len(session.statements) == 2
        archive_sql, live_sql = map(str, session.statements)
        assert "data_partitions.symbol =" in archive_sql
        assert "data_partitions.approval_status =" in archive_sql
        assert "live_data_partitions.symbol =" in live_sql
        assert "live_data_partitions.layer =" in live_sql
        assert "live_data_partitions.approval_status =" in live_sql
        assert "ORDER BY" in archive_sql
        assert "ORDER BY" in live_sql
        assert "LIMIT" in archive_sql
        assert "LIMIT" in live_sql

    asyncio.run(scenario())


def test_live_tick_partitions_do_not_substitute_for_official_archive_coverage() -> None:
    class ValuesResult(Result):
        def __init__(self, values) -> None:
            self.values = values

        def all(self):
            return self.values

        def first(self):
            return self.values[0] if self.values else None

    class SummarySession(CapturingSession):
        async def execute(self, statement):
            self.statements.append(statement)
            sql = str(statement)
            if "FROM live_data_partitions" in sql:
                return ValuesResult(["klines", "mark_price"])
            return ValuesResult([])

        async def scalar(self, statement):
            self.statements.append(statement)
            return 0

    async def scenario() -> None:
        repository = SqlAlchemyDataStateRepository(  # type: ignore[arg-type]
            SummarySession()
        )

        summary = await repository.get_symbol_summary("BTCUSDT")

        assert summary.approved_data_types == ()

    asyncio.run(scenario())


def test_symbol_api_repository_orders_and_bounds_database_work() -> None:
    async def scenario() -> None:
        session = CapturingSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]

        assert await repository.list_symbols(limit=50, offset=10) == ()

        sql = str(session.statements[0])
        assert "ORDER BY symbols.symbol" in sql
        assert "LIMIT" in sql
        assert "OFFSET" in sql

    asyncio.run(scenario())


def test_backfill_view_derives_completion_from_durable_object_states() -> None:
    class ValuesResult(Result):
        @staticmethod
        def all():
            return ["catalog_approved"]

    class JobSession(CapturingSession):
        async def get(self, _model, _identity):
            now = datetime(2026, 7, 1, tzinfo=UTC)
            return IngestionJobRow(
                id="00000000-0000-0000-0000-000000000101",
                symbol="BTCUSDT",
                dataset="kline_1m",
                status="queued",
                requested_start=now,
                requested_end=now.replace(day=2),
                created_at=now,
                updated_at=now,
            )

        async def execute(self, statement):
            self.statements.append(statement)
            return ValuesResult()

    async def scenario() -> None:
        repository = SqlAlchemyDataStateRepository(JobSession())  # type: ignore[arg-type]

        job = await repository.get_backfill(
            "00000000-0000-0000-0000-000000000101"
        )

        assert job is not None
        assert job.status == "succeeded"

    asyncio.run(scenario())
