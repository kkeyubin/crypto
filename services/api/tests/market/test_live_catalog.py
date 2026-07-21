import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from crypto_research.db.models import LiveDataPartitionRow
from crypto_research.market.binance.streams import parse_stream_message
from crypto_research.market.live_catalog import (
    LiveCatalogError,
    SqlAlchemyLiveCatalogRepository,
    approved_live_parquet_paths,
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


def aggregate_trade():
    return parse_stream_message(
        json.dumps(
            {
                "stream": "btcusdt@aggtrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1_753_099_200_010,
                    "s": "BTCUSDT",
                    "a": 42,
                    "p": "1.25",
                    "q": "2",
                    "f": 100,
                    "l": 102,
                    "T": 1_753_099_200_009,
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


def test_duckdb_boundary_only_accepts_registered_approved_normalized_parquet(
    tmp_path: Path,
) -> None:
    root = tmp_path / "market-data"
    approved = LiveDataPartitionRow(
        id="00000000-0000-0000-0000-000000000001",
        batch_id="batch-1",
        layer="normalized",
        symbol="BTCUSDT",
        dataset="agg_trades",
        partition_date="2026-07-21",
        relative_path=(
            "normalized/binance/usdm/BTCUSDT/agg_trades/"
            "date=2026-07-21/part-aaaaaaaaaaaaaaaaaaaaaaaa.parquet"
        ),
        checksum_sha256="a" * 64,
        schema_name="parquet/binance-aggregate-trade-v1",
        sort_keys=["aggregate_trade_id"],
        unique_keys=["aggregate_trade_id"],
        min_source_event_time=1,
        max_source_event_time=2,
        row_count=1,
        approval_status="approved",
    )
    unregistered = root / approved.relative_path.replace("a.parquet", "b.parquet")
    unregistered.parent.mkdir(parents=True)
    unregistered.write_bytes(b"unregistered")

    assert approved_live_parquet_paths((approved,), root) == (
        root / approved.relative_path,
    )
    assert unregistered not in approved_live_parquet_paths((approved,), root)

    raw = LiveDataPartitionRow(
        **{
            key: value
            for key, value in approved.__dict__.items()
            if not key.startswith("_sa_") and key not in {"layer", "relative_path"}
        },
        layer="raw",
        relative_path="raw/binance/usdm/BTCUSDT/agg_trades/date=2026-07-21/x.parquet",
    )
    with pytest.raises(LiveCatalogError, match="normalized"):
        approved_live_parquet_paths((raw,), root)


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
