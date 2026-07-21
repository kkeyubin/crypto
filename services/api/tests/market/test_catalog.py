import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_research.contracts.manifest import (
    ArchiveCadence,
    ArchiveDataset,
    BinanceArchiveSource,
    DataManifest,
    DataType,
    DeduplicationMethod,
    ValidationState,
)
from crypto_research.contracts.strategy import InstrumentRef
from crypto_research.db.models import DataManifestRow, DataPartitionRow, SourceObjectRow
from crypto_research.market.catalog import (
    CatalogCandidate,
    CatalogValidationError,
    InMemoryCatalogRepository,
    SecureDuckDBCatalog,
    SqlAlchemyCatalogRepository,
)

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 1, 2, tzinfo=UTC)


def manifest(
    manifest_id: str,
    source_checksum: str,
    normalized_checksum: str,
    normalized_path: str,
    *,
    start: datetime = START,
    end: datetime = END,
    cadence: ArchiveCadence = ArchiveCadence.DAILY,
) -> DataManifest:
    return DataManifest(
        manifest_id=UUID(manifest_id),
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol="BTCUSDT"
        ),
        data_type=DataType.KLINE_1M,
        start=start,
        end=end,
        retrieved_at=end + timedelta(days=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        source=BinanceArchiveSource(
            kind="binance_archive",
            cadence=cadence,
            dataset=ArchiveDataset.KLINES,
            symbol="BTCUSDT",
            interval="1m",
            period_start=start,
        ),
        raw_path=f"raw/{source_checksum}.zip",
        normalized_path=normalized_path,
        source_checksum=source_checksum,
        normalized_checksum=normalized_checksum,
        row_count=2,
        validation_state=ValidationState.VALIDATED,
        primary_key_fields=("open_time",),
        deduplication_method=DeduplicationMethod.REJECT_DUPLICATES,
        duplicates_removed=0,
    )


def test_catalog_rejects_overlapping_active_monthly_and_daily_coverage() -> None:
    async def scenario() -> None:
        repository = InMemoryCatalogRepository()
        await repository.approve(
            CatalogCandidate(
                manifest(
                    "00000000-0000-0000-0000-000000000001",
                    "a" * 64,
                    "b" * 64,
                    "normalized/month.parquet",
                    start=START,
                    end=datetime(2024, 2, 1, tzinfo=UTC),
                    cadence=ArchiveCadence.MONTHLY,
                ),
                validations(),
            )
        )
        with pytest.raises(CatalogValidationError, match="overlap"):
            await repository.approve(
                CatalogCandidate(
                    manifest(
                        "00000000-0000-0000-0000-000000000002",
                        "c" * 64,
                        "d" * 64,
                        "normalized/day.parquet",
                        start=datetime(2024, 1, 2, tzinfo=UTC),
                        end=datetime(2024, 1, 3, tzinfo=UTC),
                    ),
                    validations(),
                )
            )

    asyncio.run(scenario())


def validations(**overrides: bool) -> dict[str, bool]:
    result = {
        "checksum": True,
        "schema": True,
        "ordering": True,
        "uniqueness": True,
        "range": True,
        "row_count": True,
    }
    result.update(overrides)
    return result


def test_catalog_exposes_only_partitions_with_all_validators_approved() -> None:
    async def scenario() -> None:
        repository = InMemoryCatalogRepository()
        bad = CatalogCandidate(
            manifest=manifest(
                "00000000-0000-0000-0000-000000000101",
                "a" * 64,
                "b" * 64,
                "normalized/btc-v1.parquet",
            ),
            validation_results=validations(ordering=False),
        )
        with pytest.raises(CatalogValidationError, match="ordering"):
            await repository.approve(bad)
        assert await repository.approved("BTCUSDT", DataType.KLINE_1M, START, END) == ()

        approved = await repository.approve(
            CatalogCandidate(bad.manifest, validations())
        )
        assert approved.version == 1
        assert await repository.approved("BTCUSDT", DataType.KLINE_1M, START, END) == (
            approved,
        )

    asyncio.run(scenario())


def test_same_checksum_is_idempotent_and_replacement_creates_immutable_version() -> None:
    async def scenario() -> None:
        repository = InMemoryCatalogRepository()
        first_manifest = manifest(
            "00000000-0000-0000-0000-000000000111",
            "a" * 64,
            "b" * 64,
            "normalized/btc-v1.parquet",
        )
        first = await repository.approve(CatalogCandidate(first_manifest, validations()))
        retry = await repository.approve(
            CatalogCandidate(
                manifest(
                    "00000000-0000-0000-0000-000000000112",
                    "a" * 64,
                    "b" * 64,
                    "normalized/btc-v1.parquet",
                ),
                validations(),
            )
        )
        replacement = await repository.approve(
            CatalogCandidate(
                manifest(
                    "00000000-0000-0000-0000-000000000113",
                    "c" * 64,
                    "d" * 64,
                    "normalized/btc-v2.parquet",
                ),
                validations(),
            )
        )

        assert retry == first
        assert replacement.version == 2
        assert replacement.partition_id != first.partition_id
        assert first.parquet_path == "normalized/btc-v1.parquet"
        assert replacement.parquet_path == "normalized/btc-v2.parquet"

    asyncio.run(scenario())


def test_replacement_cannot_reuse_approved_partition_bytes() -> None:
    async def scenario() -> None:
        repository = InMemoryCatalogRepository()
        await repository.approve(
            CatalogCandidate(
                manifest(
                    "00000000-0000-0000-0000-000000000121",
                    "a" * 64,
                    "b" * 64,
                    "normalized/shared.parquet",
                ),
                validations(),
            )
        )
        with pytest.raises(CatalogValidationError, match="immutable"):
            await repository.approve(
                CatalogCandidate(
                    manifest(
                        "00000000-0000-0000-0000-000000000122",
                        "c" * 64,
                        "d" * 64,
                        "normalized/shared.parquet",
                    ),
                    validations(),
                )
            )

    asyncio.run(scenario())


def test_duckdb_reads_only_catalog_paths_beneath_root_in_deterministic_order(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "data"
        root.mkdir(mode=0o700)
        relative = "normalized/btc-v1.parquet"
        parquet = root / relative
        parquet.parent.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "open_time": [
                        int(START.timestamp() * 1000) + 60_000,
                        int(START.timestamp() * 1000),
                    ],
                    "close": [2, 1],
                }
            ),
            parquet,
        )
        normalized_checksum = hashlib.sha256(parquet.read_bytes()).hexdigest()
        repository = InMemoryCatalogRepository()
        await repository.approve(
            CatalogCandidate(
                manifest(
                    "00000000-0000-0000-0000-000000000131",
                    "a" * 64,
                    normalized_checksum,
                    relative,
                ),
                validations(),
            )
        )
        catalog = SecureDuckDBCatalog(root, repository)

        rows = await catalog.read("BTCUSDT", DataType.KLINE_1M, START, END)
        assert [row["open_time"] for row in rows] == sorted(
            row["open_time"] for row in rows
        )
        assert not hasattr(catalog, "query")

        outside = tmp_path / "outside.parquet"
        outside.write_bytes(parquet.read_bytes())
        parquet.unlink()
        parquet.symlink_to(outside)
        with pytest.raises(ValueError, match="data root"):
            await catalog.read("BTCUSDT", DataType.KLINE_1M, START, END)

    asyncio.run(scenario())


def test_duckdb_rechecks_approved_bytes_by_descriptor_before_query(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = tmp_path / "data"
        root.mkdir(mode=0o700)
        relative = "normalized/btc-v1.parquet"
        parquet = root / relative
        parquet.parent.mkdir(parents=True)
        pq.write_table(pa.table({"open_time": [int(START.timestamp() * 1000)]}), parquet)
        approved_checksum = hashlib.sha256(parquet.read_bytes()).hexdigest()
        repository = InMemoryCatalogRepository()
        await repository.approve(
            CatalogCandidate(
                manifest(
                    "00000000-0000-0000-0000-000000000132",
                    "a" * 64,
                    approved_checksum,
                    relative,
                ),
                validations(),
            )
        )
        pq.write_table(
            pa.table({"open_time": [int(START.timestamp() * 1000)], "tampered": [True]}),
            parquet,
        )

        with pytest.raises(ValueError, match="checksum"):
            await SecureDuckDBCatalog(root, repository).read(
                "BTCUSDT", DataType.KLINE_1M, START, END
            )

    asyncio.run(scenario())


def test_duckdb_reads_only_latest_approved_source_replacement(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = tmp_path / "data"
        root.mkdir(mode=0o700)
        repository = InMemoryCatalogRepository()
        for version, close, source_checksum, manifest_id in (
            (1, 1, "a" * 64, "00000000-0000-0000-0000-000000000141"),
            (2, 2, "b" * 64, "00000000-0000-0000-0000-000000000142"),
        ):
            relative = f"normalized/btc-v{version}.parquet"
            parquet = root / relative
            parquet.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.table(
                    {
                        "open_time": [int(START.timestamp() * 1000)],
                        "close": [close],
                    }
                ),
                parquet,
            )
            checksum = hashlib.sha256(parquet.read_bytes()).hexdigest()
            await repository.approve(
                CatalogCandidate(
                    manifest(
                        manifest_id,
                        source_checksum,
                        checksum,
                        relative,
                    ),
                    validations(),
                )
            )

        rows = await SecureDuckDBCatalog(root, repository).read(
            "BTCUSDT", DataType.KLINE_1M, START, END
        )
        assert [row["close"] for row in rows] == [2]

    asyncio.run(scenario())


def test_catalog_rejects_sub_millisecond_query_boundaries() -> None:
    async def scenario() -> None:
        repository = InMemoryCatalogRepository()
        with pytest.raises(ValueError, match="millisecond-aligned"):
            await repository.approved(
                "BTCUSDT",
                DataType.KLINE_1M,
                START.replace(microsecond=1),
                END,
            )

    asyncio.run(scenario())


def test_sql_catalog_flushes_source_then_partition_then_manifest() -> None:
    class EmptyResult:
        def first(self):
            return None

        def all(self):
            return []

        def scalars(self):
            return self

    class ForeignKeyOrderingSession:
        def __init__(self) -> None:
            self.pending: list[object] = []
            self.persisted: list[object] = []
            self.flush_batches: list[tuple[type[object], ...]] = []

        async def execute(self, _statement):
            return EmptyResult()

        def add(self, row: object) -> None:
            self.pending.append(row)

        async def flush(self) -> None:
            if any(isinstance(row, DataManifestRow) for row in self.pending) and not any(
                isinstance(row, DataPartitionRow) for row in self.persisted
            ):
                raise RuntimeError("manifest FK observed before partition INSERT")
            if any(isinstance(row, DataPartitionRow) for row in self.pending) and not any(
                isinstance(row, SourceObjectRow) for row in self.persisted
            ):
                raise RuntimeError("partition FK observed before source INSERT")
            self.flush_batches.append(tuple(type(row) for row in self.pending))
            self.persisted.extend(self.pending)
            self.pending.clear()

    async def scenario() -> None:
        session = ForeignKeyOrderingSession()
        repository = SqlAlchemyCatalogRepository(session)  # type: ignore[arg-type]

        await repository.approve(
            CatalogCandidate(
                manifest(
                    "00000000-0000-0000-0000-000000000151",
                    "a" * 64,
                    "b" * 64,
                    "normalized/ordered.parquet",
                ),
                validations(),
            )
        )

        assert session.flush_batches == [
            (SourceObjectRow,),
            (DataPartitionRow,),
            (DataManifestRow,),
        ]
        stored = next(
            row for row in session.persisted if isinstance(row, DataManifestRow)
        )
        assert "resolved_url" not in stored.manifest["source"]

    asyncio.run(scenario())
