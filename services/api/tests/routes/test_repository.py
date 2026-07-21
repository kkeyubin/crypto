import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

from crypto_research.contracts.manifest import (
    ArchiveCadence,
    ArchiveDataset,
    BinanceArchiveSource,
    DataManifest,
    DataType,
    DeduplicationMethod,
    MissingInterval,
    ValidationState,
)
from crypto_research.contracts.strategy import InstrumentRef
from crypto_research.db.models import (
    DataManifestRow,
    DataPartitionRow,
    IngestionJobRow,
    SymbolMetadataSnapshotRow,
)
from crypto_research.db.repositories import (
    SqlAlchemyDataStateRepository,
    _effective_job_status,
)


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

    async def get(self, model, identity, **_kwargs):
        if model.__name__ == "SymbolRow":
            from crypto_research.db.models import SymbolRow

            return SymbolRow(
                symbol=identity,
                enabled=True,
                history_start=datetime(2026, 6, 1, tzinfo=UTC),
                history_end=datetime(2026, 7, 1, tzinfo=UTC),
                include_agg_trades=False,
            )
        return None


class ValuesResult(Result):
    def __init__(self, values) -> None:
        self.values = values

    def all(self):
        return self.values

    def scalars(self):
        return self

    def first(self):
        return self.values[0] if self.values else None


def archive_manifest(data_type: DataType, manifest_id: str) -> DataManifest:
    start = datetime(2026, 6, 1, tzinfo=UTC)
    end = datetime(2026, 7, 1, tzinfo=UTC)
    archive_dataset = {
        DataType.KLINE_1M: ArchiveDataset.KLINES,
        DataType.MARK_PRICE: ArchiveDataset.MARK_PRICE_KLINES,
        DataType.FUNDING: ArchiveDataset.FUNDING_RATE,
    }[data_type]
    return DataManifest(
        manifest_id=UUID(manifest_id),
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol="BTCUSDT"
        ),
        data_type=data_type,
        start=start,
        end=end,
        retrieved_at=end + timedelta(days=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        source=BinanceArchiveSource(
            kind="binance_archive",
            cadence=ArchiveCadence.MONTHLY,
            dataset=archive_dataset,
            symbol="BTCUSDT",
            interval=(None if data_type is DataType.FUNDING else "1m"),
            period_start=start,
        ),
        raw_path=f"raw/{data_type.value}.zip",
        normalized_path=f"normalized/{data_type.value}.parquet",
        source_checksum="a" * 64,
        normalized_checksum="b" * 64,
        row_count=1,
        validation_state=ValidationState.VALIDATED,
        primary_key_fields=("event_time",),
        deduplication_method=DeduplicationMethod.REJECT_DUPLICATES,
        duplicates_removed=0,
    )


def archive_row(data_type: DataType, suffix: int):
    manifest_id = f"00000000-0000-0000-0000-{suffix:012d}"
    manifest = archive_manifest(data_type, manifest_id)
    partition = DataPartitionRow(
        id=f"10000000-0000-0000-0000-{suffix:012d}",
        symbol="BTCUSDT",
        dataset=data_type.value,
        partition_date="2026-06-01",
        version=1,
        source_object_id=f"20000000-0000-0000-0000-{suffix:012d}",
        checksum_sha256="b" * 64,
        parquet_path=manifest.normalized_path,
        approval_status="approved",
    )
    stored = DataManifestRow(
        manifest_id=manifest_id,
        partition_id=partition.id,
        source_object_id=partition.source_object_id,
        source_url=manifest.source.resolved_url,
        source_checksum=manifest.source_checksum,
        manifest=manifest.model_dump(mode="json", exclude_computed_fields=True),
    )
    return partition, stored


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


def test_symbol_summary_uses_current_validated_manifests_with_bounded_sql() -> None:
    class SummarySession(CapturingSession):
        async def execute(self, statement):
            self.statements.append(statement)
            sql = str(statement)
            if "JOIN data_manifests" in sql:
                return ValuesResult(
                    [
                        archive_row(DataType.KLINE_1M, 1),
                        archive_row(DataType.MARK_PRICE, 2),
                        archive_row(DataType.FUNDING, 3),
                    ]
                )
            return ValuesResult([])

        async def scalar(self, statement):
            self.statements.append(statement)
            return 0

    async def scenario() -> None:
        session = SummarySession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]

        summary = await repository.get_symbol_summary("BTCUSDT")

        assert {
            (item.dataset, item.start, item.end)
            for item in summary.archive_intervals
        } == {
            (
                data_type.value,
                datetime(2026, 6, 1, tzinfo=UTC),
                datetime(2026, 7, 1, tzinfo=UTC),
            )
            for data_type in (
                DataType.KLINE_1M,
                DataType.MARK_PRICE,
                DataType.FUNDING,
            )
        }
        coverage_sql = next(
            str(statement)
            for statement in session.statements
            if "JOIN data_manifests" in str(statement)
        )
        assert "data_partitions.symbol IN" in coverage_sql
        assert "data_partitions.dataset IN" in coverage_sql
        assert "data_partitions.partition_date >=" in coverage_sql
        assert "data_partitions.partition_date <=" in coverage_sql
        assert "max(" in coverage_sql.lower()

    asyncio.run(scenario())


def test_symbol_summary_does_not_cover_manifest_declared_missing_intervals() -> None:
    class MissingIntervalSession(CapturingSession):
        async def execute(self, statement):
            self.statements.append(statement)
            if "JOIN data_manifests" in str(statement):
                partition, stored = archive_row(DataType.KLINE_1M, 31)
                manifest = archive_manifest(
                    DataType.KLINE_1M,
                    "00000000-0000-0000-0000-000000000031",
                ).model_copy(
                    update={
                        "missing_intervals": (
                            MissingInterval(
                                start=datetime(2026, 6, 10, tzinfo=UTC),
                                end=datetime(2026, 6, 11, tzinfo=UTC),
                            ),
                        )
                    }
                )
                stored.manifest = manifest.model_dump(
                    mode="json", exclude_computed_fields=True
                )
                return ValuesResult([(partition, stored)])
            return ValuesResult([])

    async def scenario() -> None:
        repository = SqlAlchemyDataStateRepository(  # type: ignore[arg-type]
            MissingIntervalSession()
        )

        summary = await repository.get_symbol_summary("BTCUSDT")

        assert [
            (item.start, item.end) for item in summary.archive_intervals
        ] == [
            (
                datetime(2026, 6, 1, tzinfo=UTC),
                datetime(2026, 6, 10, tzinfo=UTC),
            ),
            (
                datetime(2026, 6, 11, tzinfo=UTC),
                datetime(2026, 7, 1, tzinfo=UTC),
            ),
        ]

    asyncio.run(scenario())


def test_symbol_summary_trusts_only_fresh_validated_official_matching_metadata() -> None:
    captured_at = datetime.now(UTC)
    trusted_payload = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
            }
        ]
    }

    class MetadataSession(CapturingSession):
        def __init__(self, snapshot: SymbolMetadataSnapshotRow) -> None:
            super().__init__()
            self.snapshot = snapshot

        async def execute(self, statement):
            self.statements.append(statement)
            if "FROM symbol_metadata_snapshots" in str(statement):
                return ValuesResult([self.snapshot])
            return ValuesResult([])

        async def scalar(self, statement):
            self.statements.append(statement)
            return 0

    async def verified(snapshot: SymbolMetadataSnapshotRow) -> bool:
        repository = SqlAlchemyDataStateRepository(  # type: ignore[arg-type]
            MetadataSession(snapshot)
        )
        return (await repository.get_symbol_summary("BTCUSDT")).metadata_verified

    async def scenario() -> None:
        good = SymbolMetadataSnapshotRow(
            id="00000000-0000-0000-0000-000000000401",
            symbol="BTCUSDT",
            source="binance_usdm_exchange_info",
            payload=trusted_payload,
            captured_at=captured_at,
        )
        assert await verified(good) is True

        bad_snapshots = (
            SymbolMetadataSnapshotRow(
                id="00000000-0000-0000-0000-000000000402",
                symbol="BTCUSDT",
                source="untrusted_cache",
                payload=trusted_payload,
                captured_at=captured_at,
            ),
            SymbolMetadataSnapshotRow(
                id="00000000-0000-0000-0000-000000000403",
                symbol="BTCUSDT",
                source="binance_usdm_exchange_info",
                payload={},
                captured_at=captured_at,
            ),
            SymbolMetadataSnapshotRow(
                id="00000000-0000-0000-0000-000000000404",
                symbol="BTCUSDT",
                source="binance_usdm_exchange_info",
                payload={
                    "symbols": [
                        {
                            "symbol": "PEPEUSDT",
                            "contractType": "PERPETUAL",
                            "status": "TRADING",
                        }
                    ]
                },
                captured_at=captured_at,
            ),
            SymbolMetadataSnapshotRow(
                id="00000000-0000-0000-0000-000000000405",
                symbol="BTCUSDT",
                source="binance_usdm_exchange_info",
                payload=trusted_payload,
                captured_at=captured_at - timedelta(hours=24, microseconds=1),
            ),
        )
        assert [await verified(snapshot) for snapshot in bad_snapshots] == [
            False,
            False,
            False,
            False,
        ]

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


def test_effective_job_status_always_projects_durable_object_states() -> None:
    assert _effective_job_status("succeeded", ("failed", "catalog_approved")) == "failed"
    assert _effective_job_status("failed", ("catalog_approved",)) == "succeeded"
    assert _effective_job_status(
        "succeeded", ("downloading", "catalog_approved")
    ) == "running"
    assert _effective_job_status(
        "running", ("planned", "source_pending", "catalog_approved")
    ) == "queued"
    assert _effective_job_status("cancelled", ()) == "cancelled"


def test_failed_job_health_uses_the_same_object_state_projection() -> None:
    class HealthSession(CapturingSession):
        async def scalar(self, statement):
            self.statements.append(statement)
            return 0

    async def scenario() -> None:
        session = HealthSession()
        repository = SqlAlchemyDataStateRepository(session)  # type: ignore[arg-type]

        assert await repository.count_failed_jobs() == 0

        assert len(session.statements) == 1
        sql = str(session.statements[0]).lower()
        assert "backfill_objects" in sql
        assert "case" in sql
        assert "failed" in sql

    asyncio.run(scenario())
