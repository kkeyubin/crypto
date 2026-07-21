import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from crypto_research.db.models import LiveDataPartitionRow
from crypto_research.market.binance.streams import parse_stream_message
from crypto_research.market.live_catalog import (
    LiveCatalogError,
    SecureLiveDuckDBCatalog,
    SqlAlchemyLiveCatalogRepository,
)
from crypto_research.market.live_storage import LiveStorage


class FakeSession:
    def __init__(self) -> None:
        self.rows: list[object] = []

    async def get(self, model, identity):
        return next(
            (
                row
                for row in self.rows
                if isinstance(row, model) and getattr(row, "id", None) == identity
            ),
            None,
        )

    def add(self, row: object) -> None:
        self.rows.append(row)

    async def flush(self) -> None:
        return None


def aggregate_trade(identity: int = 42):
    return parse_stream_message(
        json.dumps(
            {
                "stream": "btcusdt@aggtrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_753_099_200_010 + identity,
                    "s": "BTCUSDT",
                    "a": identity,
                    "p": "1.25",
                    "q": "2",
                    "f": identity,
                    "l": identity,
                    "T": 1_753_099_200_009 + identity,
                    "m": False,
                },
            }
        ),
        datetime(2026, 7, 21, 12, 0, 1, tzinfo=UTC),
    )


def test_live_write_result_registers_complete_approved_manifest_rows(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "market-data"
        storage = LiveStorage(root)
        lease = storage.acquire_writer("worker-a")
        storage.accept(lease, aggregate_trade())
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        session = FakeSession()
        repository = SqlAlchemyLiveCatalogRepository(session)  # type: ignore[arg-type]

        registered = await repository.register_batch(result)
        replayed = await repository.register_batch(result)

        assert registered == replayed
        assert len(session.rows) == 2
        normalized = next(
            row
            for row in session.rows
            if isinstance(row, LiveDataPartitionRow) and row.layer == "normalized"
        )
        assert normalized.batch_id == result.batch_id
        assert normalized.relative_path.endswith(".parquet")
        assert normalized.checksum_sha256 == result.normalized[0].sha256
        assert normalized.schema_name == "parquet/binance-aggregate-trade-v1"
        assert normalized.sort_keys == ["aggregate_trade_id"]
        assert normalized.unique_keys == ["aggregate_trade_id"]
        assert normalized.row_count == 1
        assert normalized.approval_status == "approved"
        lease.close()

    asyncio.run(scenario())


def test_catalog_identity_is_immutable(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = LiveStorage(tmp_path / "market-data")
        lease = storage.acquire_writer("worker-a")
        storage.accept(lease, aggregate_trade())
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        session = FakeSession()
        repository = SqlAlchemyLiveCatalogRepository(session)  # type: ignore[arg-type]
        await repository.register_batch(result)
        normalized = next(
            row
            for row in session.rows
            if isinstance(row, LiveDataPartitionRow) and row.layer == "normalized"
        )
        normalized.checksum_sha256 = "f" * 64

        with pytest.raises(LiveCatalogError, match="immutable"):
            await repository.register_batch(result)
        lease.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"partition_date": "2025-99-99"}, "partition date"),
        ({"symbol": "BTC-USDT"}, "symbol"),
        ({"dataset": "depth"}, "dataset"),
        (
            {"schema_name": "parquet/binance-kline-1m-v1"},
            "schema contract",
        ),
        ({"sort_keys": ()}, "query contract"),
        (
            {"unique_keys": ("aggregate_trade_id", "")},
            "query contract",
        ),
        (
            {
                "relative_path": (
                    "normalized/binance/usdm/BTCUSDT/agg_trades/"
                    "date=2025-07-21/part-ffffffffffffffffffffffff.parquet"
                )
            },
            "checksum",
        ),
    ],
)
def test_repository_rejects_adversarial_live_partition_contracts(
    tmp_path: Path, changes: dict[str, object], message: str
) -> None:
    async def scenario() -> None:
        storage = LiveStorage(tmp_path / "market-data")
        lease = storage.acquire_writer("worker-a")
        storage.accept(lease, aggregate_trade())
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        changed = replace(result.normalized[0], **changes)
        invalid = replace(result, normalized=(changed,))
        repository = SqlAlchemyLiveCatalogRepository(  # type: ignore[arg-type]
            FakeSession()
        )

        with pytest.raises(LiveCatalogError, match=message):
            await repository.register_batch(invalid)
        lease.close()

    asyncio.run(scenario())


def test_secure_duckdb_reads_only_repository_approved_descriptors(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "market-data"
        storage = LiveStorage(root)
        lease = storage.acquire_writer("worker-a")
        for identity in (43, 42):
            storage.accept(lease, aggregate_trade(identity))
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        session = FakeSession()
        approved_rows = await SqlAlchemyLiveCatalogRepository(  # type: ignore[arg-type]
            session
        ).register_batch(result)
        normalized = tuple(row for row in approved_rows if row.layer == "normalized")

        class ApprovedRows:
            def __init__(self) -> None:
                self.calls = []

            async def list_approved_normalized(self, **kwargs):
                self.calls.append(kwargs)
                return normalized

        repository = ApprovedRows()
        unregistered = result.normalized[0].path.with_name(
            "part-ffffffffffffffffffffffff.parquet"
        )
        unregistered.write_bytes(result.normalized[0].path.read_bytes())
        catalog = SecureLiveDuckDBCatalog(root, repository)  # type: ignore[arg-type]

        rows = await catalog.read(
            "BTCUSDT",
            "agg_trades",
            1_753_099_200_000,
            1_753_099_201_000,
        )

        assert [row["aggregate_trade_id"] for row in rows] == [42, 43]
        assert repository.calls == [
            {
                "symbol": "BTCUSDT",
                "dataset": "agg_trades",
                "start_event_time": 1_753_099_200_000,
                "end_event_time": 1_753_099_201_000,
            }
        ]
        assert not hasattr(catalog, "query")
        assert not hasattr(catalog, "approved_parquet_paths")
        assert unregistered.exists()
        lease.close()

    asyncio.run(scenario())


def test_secure_duckdb_rejects_repository_rows_outside_the_request(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "market-data"
        storage = LiveStorage(root)
        lease = storage.acquire_writer("worker-a")
        storage.accept(lease, aggregate_trade())
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        session = FakeSession()
        rows = await SqlAlchemyLiveCatalogRepository(  # type: ignore[arg-type]
            session
        ).register_batch(result)
        approved = next(row for row in rows if row.layer == "normalized")
        approved.dataset = "klines"

        class WrongDataset:
            async def list_approved_normalized(self, **_kwargs):
                return (approved,)

        with pytest.raises(LiveCatalogError, match="unapproved partition"):
            await SecureLiveDuckDBCatalog(  # type: ignore[arg-type]
                root, WrongDataset()
            ).read(
                "BTCUSDT",
                "agg_trades",
                1_753_099_200_000,
                1_753_099_201_000,
            )
        lease.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("attack", ["symlink", "checksum"])
def test_secure_duckdb_rejects_catalog_file_substitution(
    tmp_path: Path, attack: str
) -> None:
    async def scenario() -> None:
        root = tmp_path / "market-data"
        storage = LiveStorage(root)
        lease = storage.acquire_writer("worker-a")
        storage.accept(lease, aggregate_trade())
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        session = FakeSession()
        rows = await SqlAlchemyLiveCatalogRepository(  # type: ignore[arg-type]
            session
        ).register_batch(result)
        approved = tuple(row for row in rows if row.layer == "normalized")

        class ApprovedRows:
            async def list_approved_normalized(self, **_kwargs):
                return approved

        parquet = result.normalized[0].path
        if attack == "symlink":
            outside = tmp_path / "outside.parquet"
            outside.write_bytes(parquet.read_bytes())
            parquet.unlink()
            parquet.symlink_to(outside)
            expected = "data root"
        else:
            parquet.write_bytes(parquet.read_bytes() + b"tampered")
            assert hashlib.sha256(parquet.read_bytes()).hexdigest() != approved[0].checksum_sha256
            expected = "checksum"

        with pytest.raises(LiveCatalogError, match=expected):
            await SecureLiveDuckDBCatalog(  # type: ignore[arg-type]
                root, ApprovedRows()
            ).read(
                "BTCUSDT",
                "agg_trades",
                1_753_099_200_000,
                1_753_099_201_000,
            )
        lease.close()

    asyncio.run(scenario())


def test_migration_0003_creates_independent_live_catalog() -> None:
    migration = (
        Path(__file__).resolve().parents[2]
        / "migrations/versions/20260721_0003_live_data_catalog.py"
    ).read_text()

    assert 'down_revision = "20260721_0002"' in migration
    assert '"live_data_partitions"' in migration
    assert '"source_object_id"' not in migration
    assert '"unique_keys"' in migration
    assert '"min_source_event_time"' in migration
    assert "ck_live_data_partitions_partition_date" in migration
    assert "ck_live_data_partitions_dataset" in migration
    assert "json_typeof(sort_keys)" in migration
    assert "json_array_length(unique_keys)" in migration
    assert "left(checksum_sha256, 24)" in migration
