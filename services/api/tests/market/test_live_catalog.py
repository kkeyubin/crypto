import asyncio
import hashlib
import json
import os
import subprocess
import sys
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
from crypto_research.market.live_storage import LiveStorage, StoredLivePartition


class FakeResult:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def scalars(self):
        return self

    def all(self) -> list[object]:
        return self._rows


class FakeSession:
    def __init__(self) -> None:
        self.rows: list[object] = []
        self.statements: list[object] = []

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

    async def execute(self, statement) -> FakeResult:
        self.statements.append(statement)
        sql = str(statement)
        if "live_data_partitions.batch_id" not in sql:
            return FakeResult([])
        parameters = statement.compile().params
        batch_id = next(
            value
            for key, value in parameters.items()
            if key.startswith("batch_id")
        )
        return FakeResult(
            [
                row
                for row in self.rows
                if isinstance(row, LiveDataPartitionRow)
                and row.batch_id == batch_id
            ]
        )

    async def flush(self) -> None:
        return None


def valid_artifact_variant(
    part: StoredLivePartition,
    *,
    checksum: str | None = None,
    **changes: object,
) -> StoredLivePartition:
    changed = replace(part, **changes)
    selected_checksum = checksum or changed.sha256
    suffix = ".parquet" if changed.layer == "normalized" else ".ndjson.gz"
    filename = f"part-{selected_checksum[:24]}{suffix}"
    relative_path = Path(
        changed.layer,
        "binance",
        "usdm",
        changed.symbol,
        changed.dataset,
        f"date={changed.partition_date}",
        filename,
    ).as_posix()
    data_root = part.path
    for _ in Path(part.relative_path).parts:
        data_root = data_root.parent
    return replace(
        changed,
        path=data_root / relative_path,
        sha256=selected_checksum,
        relative_path=relative_path,
    )


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


def midnight_final_kline():
    open_time = int(
        datetime(2026, 7, 21, 23, 59, tzinfo=UTC).timestamp() * 1_000
    )
    return parse_stream_message(
        json.dumps(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "E": open_time + 60_500,
                    "s": "BTCUSDT",
                    "k": {
                        "t": open_time,
                        "T": open_time + 59_999,
                        "s": "BTCUSDT",
                        "i": "1m",
                        "o": "1.23",
                        "c": "1.25",
                        "h": "1.26",
                        "l": "1.22",
                        "v": "10",
                        "n": 4,
                        "x": True,
                        "q": "12.5",
                        "V": "4",
                        "Q": "5",
                    },
                },
            }
        ),
        datetime(2026, 7, 22, 0, 0, 1, tzinfo=UTC),
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
        assert sum(
            "pg_advisory_xact_lock" in str(statement)
            for statement in session.statements
        ) == 2
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
        assert normalized.min_canonical_time == 1_753_099_200_051
        assert normalized.max_canonical_time == 1_753_099_200_051
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
    "mutation",
    [
        "replace-checksum-path",
        "add-artifact",
        "delete-artifact",
        "change-row-count",
        "change-range",
        "change-layer",
        "change-schema-contract",
    ],
)
def test_batch_artifact_set_is_immutable(tmp_path: Path, mutation: str) -> None:
    async def scenario() -> None:
        storage = LiveStorage(tmp_path / "market-data")
        lease = storage.acquire_writer("worker-a")
        storage.accept(lease, aggregate_trade())
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        original = result.normalized[0]
        replacement = valid_artifact_variant(original, checksum="c" * 64)
        if mutation == "replace-checksum-path":
            conflicting = replace(result, normalized=(replacement,))
        elif mutation == "add-artifact":
            conflicting = replace(
                result, normalized=(*result.normalized, replacement)
            )
        elif mutation == "delete-artifact":
            conflicting = replace(result, normalized=())
        elif mutation == "change-row-count":
            conflicting = replace(
                result,
                normalized=(replace(original, row_count=original.row_count + 1),),
            )
        elif mutation == "change-range":
            conflicting = replace(
                result,
                normalized=(
                    replace(
                        original,
                        max_canonical_time=original.max_canonical_time + 1,
                    ),
                ),
            )
        elif mutation == "change-layer":
            conflicting = replace(
                result,
                normalized=(
                    valid_artifact_variant(
                        original,
                        checksum="d" * 64,
                        layer="raw",
                        schema_name="ndjson/binance-stream-event-v1",
                        sort_keys=(
                            "source_event_time",
                            "source_id",
                            "event_key",
                        ),
                        unique_keys=("event_key",),
                    ),
                ),
            )
        else:
            conflicting = replace(
                result,
                normalized=(
                    valid_artifact_variant(
                        original,
                        checksum="e" * 64,
                        dataset="mark_price",
                        schema_name="parquet/binance-mark-price-v1",
                        sort_keys=("event_time",),
                        unique_keys=("event_time",),
                    ),
                ),
            )
        session = FakeSession()
        repository = SqlAlchemyLiveCatalogRepository(session)  # type: ignore[arg-type]
        registered = await repository.register_batch(result)

        with pytest.raises(LiveCatalogError, match="immutable"):
            await repository.register_batch(conflicting)

        assert session.rows == list(registered)
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
        ({"min_canonical_time": None}, "canonical range"),
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


def test_midnight_final_kline_is_selected_only_by_its_canonical_day(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = tmp_path / "market-data"
        storage = LiveStorage(root)
        lease = storage.acquire_writer("worker-a")
        storage.accept(lease, midnight_final_kline())
        result = storage.publish_next_batch(lease, max_events=10)
        assert result is not None
        rows = await SqlAlchemyLiveCatalogRepository(  # type: ignore[arg-type]
            FakeSession()
        ).register_batch(result)
        normalized = next(row for row in rows if row.layer == "normalized")

        class CanonicalRows:
            async def list_approved_normalized(self, **query):
                if (
                    normalized.max_canonical_time >= query["start_event_time"]
                    and normalized.min_canonical_time < query["end_event_time"]
                ):
                    return (normalized,)
                return ()

        catalog = SecureLiveDuckDBCatalog(  # type: ignore[arg-type]
            root, CanonicalRows()
        )
        day_one = int(
            datetime(2026, 7, 21, tzinfo=UTC).timestamp() * 1_000
        )
        day_two = day_one + 86_400_000
        day_three = day_two + 86_400_000

        canonical_day = await catalog.read(
            "BTCUSDT", "klines", day_one, day_two
        )
        source_event_day = await catalog.read(
            "BTCUSDT", "klines", day_two, day_three
        )

        assert len(canonical_day) == 1
        assert canonical_day[0]["open_time"] == day_two - 60_000
        assert source_event_day == ()
        lease.close()

    asyncio.run(scenario())


def test_repository_selects_and_orders_shards_on_canonical_axis() -> None:
    class EmptyScalars:
        def all(self):
            return []

    class EmptyResult:
        def scalars(self):
            return EmptyScalars()

    class CapturingSession:
        statement = None

        async def execute(self, statement):
            self.statement = statement
            return EmptyResult()

    async def scenario() -> None:
        session = CapturingSession()
        repository = SqlAlchemyLiveCatalogRepository(session)  # type: ignore[arg-type]

        await repository.list_approved_normalized(
            symbol="BTCUSDT",
            dataset="klines",
            start_event_time=100,
            end_event_time=200,
        )

        sql = str(session.statement)
        assert "max_canonical_time >=" in sql
        assert "min_canonical_time <" in sql
        assert "ORDER BY live_data_partitions.min_canonical_time" in sql

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
    migration_path = (
        Path(__file__).resolve().parents[2]
        / "migrations/versions/20260721_0003_live_data_catalog.py"
    )
    migration_bytes = migration_path.read_bytes()
    migration = migration_bytes.decode()

    assert hashlib.sha256(migration_bytes).hexdigest() == (
        "08609cb5e2291093ad4178ad25439aaadc27f66c6a2dda88bab433e908d5ac6b"
    )
    assert 'down_revision = "20260721_0002"' in migration
    assert '"live_data_partitions"' in migration
    assert '"source_object_id"' not in migration
    assert '"unique_keys"' in migration
    assert '"min_source_event_time"' in migration


def test_migration_0004_adds_canonical_ranges_and_fails_closed_on_old_rows() -> None:
    api_root = Path(__file__).resolve().parents[2]
    migration_path = (
        api_root
        / "migrations/versions/20260721_0004_live_canonical_time.py"
    )
    migration = migration_path.read_text()

    assert 'down_revision = "20260721_0003"' in migration
    assert '"min_canonical_time"' in migration
    assert '"max_canonical_time"' in migration
    assert "run DELETE FROM " in migration
    assert '"live_data_partitions, rerun migration' in migration
    assert "ck_live_data_partitions_partition_date" in migration
    assert "left(checksum_sha256, 24)" in migration

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(api_root / "alembic.ini"),
            "upgrade",
            "head",
            "--sql",
        ],
        cwd=api_root,
        env=os.environ
        | {
            "CRYPTO_DATABASE_URL": (
                "postgresql+asyncpg://crypto:crypto@localhost/crypto_research"
            )
        },
        check=True,
        capture_output=True,
        text=True,
    )

    assert "20260721_0003 -> 20260721_0004" in completed.stderr
    assert "ADD COLUMN min_canonical_time" in completed.stdout
    assert "live_data_partitions contains rows without canonical ranges" in (
        completed.stdout
    )
