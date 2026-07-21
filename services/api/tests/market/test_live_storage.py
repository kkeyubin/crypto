import gzip
import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from crypto_research.market.binance.normalization import AGG_TRADE_SCHEMA, KLINE_SCHEMA
from crypto_research.market.binance.streams import parse_stream_message
from crypto_research.market.live_storage import (
    BOOK_TICKER_SCHEMA,
    MARK_PRICE_SCHEMA,
    LiveStorage,
    LiveStorageError,
    LiveWriterUnavailable,
)

RECEIVED_AT = datetime(2026, 7, 21, 12, 0, 1, tzinfo=UTC)


def event(stream: str, data: dict[str, object]):
    return parse_stream_message(
        json.dumps({"stream": stream, "data": data}), RECEIVED_AT
    )


def kline(*, closed: bool):
    return event(
        "btcusdt@kline_1m",
        {
            "e": "kline",
            "E": 1_753_099_200_999,
            "s": "BTCUSDT",
            "k": {
                "t": 1_753_099_200_000,
                "T": 1_753_099_259_999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "1.230000000000000000",
                "c": "1.250000000000000000",
                "h": "1.260000000000000000",
                "l": "1.220000000000000000",
                "v": "10.000000000000000000",
                "n": 4,
                "x": closed,
                "q": "12.500000000000000000",
                "V": "4.000000000000000000",
                "Q": "5.000000000000000000",
            },
        },
    )


def aggregate_trade(identity: int = 42):
    return event(
        "pepeusdt@aggtrade",
        {
            "e": "aggTrade",
            "E": 1_753_099_200_010 + identity,
            "s": "PEPEUSDT",
            "a": identity,
            "p": "0.000012340000000000",
            "q": "1000000.000000000000000000",
            "f": identity,
            "l": identity,
            "T": 1_753_099_200_009 + identity,
            "m": True,
        },
    )


def test_batch_publishes_immutable_raw_ndjson_and_normalized_parquet_shards(
    tmp_path: Path,
) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")

    result = storage.persist_batch(lease, (kline(closed=True), aggregate_trade()))

    assert len(result.raw) == 2
    assert all(part.path.name.startswith("part-") for part in result.raw)
    assert all(part.path.name.endswith(".ndjson.gz") for part in result.raw)
    assert len(result.normalized) == 2
    assert all(part.path.name.startswith("part-") for part in result.normalized)
    assert all(part.path.name.endswith(".parquet") for part in result.normalized)
    kline_part = next(part for part in result.normalized if "/klines/" in str(part.path))
    table = pq.ParquetFile(kline_part.path).read()
    assert table.schema.equals(KLINE_SCHEMA, check_metadata=True)
    assert table.column("open").to_pylist() == [Decimal("1.230000000000000000")]
    kline_raw = next(part for part in result.raw if "/klines/" in str(part.path))
    rows = [json.loads(line) for line in gzip.decompress(kline_raw.path.read_bytes()).splitlines()]
    assert rows[0]["payload"]["e"] == "kline"
    assert not list(storage.data_root.rglob("live.parquet"))
    lease.close()


def test_batch_keeps_in_progress_kline_raw_without_normalizing(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")

    result = storage.persist_batch(lease, (kline(closed=False),))

    assert result.raw[0].row_count == 1
    assert result.normalized == ()
    lease.close()


def test_replay_identity_ignores_receive_time_but_conflicting_payload_fails(
    tmp_path: Path,
) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    parsed = aggregate_trade()
    first = storage.persist_batch(lease, (parsed,))
    replay = replace(parsed, receive_time=parsed.receive_time + timedelta(seconds=30))

    second = storage.persist_batch(lease, (replay,))

    assert second.replayed_count == 1
    assert second.raw == first.raw
    conflicting_raw = {**parsed.raw, "p": "0.000099990000000000"}
    conflicting = replace(parsed, raw=conflicting_raw)
    with pytest.raises(LiveStorageError, match="conflicts"):
        storage.persist_batch(lease, (conflicting,))
    lease.close()


def test_same_identity_repeated_inside_one_batch_is_written_once(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    parsed = aggregate_trade()
    replay = replace(parsed, receive_time=parsed.receive_time + timedelta(seconds=1))

    result = storage.persist_batch(lease, (parsed, replay))

    assert result.replayed_count == 1
    assert result.raw[0].row_count == 1
    assert pq.ParquetFile(result.normalized[0].path).read().num_rows == 1
    lease.close()


def test_one_authoritative_writer_is_enforced_across_storage_instances(
    tmp_path: Path,
) -> None:
    root = tmp_path / "market-data"
    first_storage = LiveStorage(root)
    second_storage = LiveStorage(root)
    first = first_storage.acquire_writer("worker-a")

    with pytest.raises(LiveWriterUnavailable, match="authoritative"):
        second_storage.acquire_writer("worker-b")

    first.close()
    second = second_storage.acquire_writer("worker-b")
    second.close()


def test_authoritative_writer_lock_is_enforced_across_processes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "market-data"
    storage = LiveStorage(root)
    lease = storage.acquire_writer("parent-worker")
    script = """
import sys
from pathlib import Path
from crypto_research.market.live_storage import LiveStorage, LiveWriterUnavailable
try:
    LiveStorage(Path(sys.argv[1])).acquire_writer("child-worker")
except LiveWriterUnavailable:
    raise SystemExit(23)
raise SystemExit(0)
"""

    completed = subprocess.run(
        [sys.executable, "-c", script, str(root)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 23, completed.stderr
    lease.close()


def test_batch_rolls_one_shard_per_partition_without_lost_rows(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    events = tuple(aggregate_trade(index) for index in range(24))

    result = storage.persist_batch(lease, events)

    assert len(result.raw) == 1
    assert len(result.normalized) == 1
    assert result.raw[0].row_count == 24
    table = pq.ParquetFile(result.normalized[0].path).read()
    assert table.schema.equals(AGG_TRADE_SCHEMA, check_metadata=True)
    assert table.num_rows == 24
    assert table.column("aggregate_trade_id").to_pylist() == list(range(24))
    lease.close()


def test_mark_and_book_ticker_remain_decimal_exact_in_separate_schemas(
    tmp_path: Path,
) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    mark = event(
        "btcusdt@markprice@1s",
        {
            "e": "markPriceUpdate",
            "E": 1_753_099_200_999,
            "s": "BTCUSDT",
            "p": "117415.500000000000000000",
            "i": "117400.100000000000000000",
            "P": "117390.000000000000000000",
            "r": "0.000100000000000000",
            "T": 1_753_128_000_000,
        },
    )
    ticker = event(
        "btcusdt@bookticker",
        {
            "e": "bookTicker",
            "E": 1_753_099_200_011,
            "T": 1_753_099_200_010,
            "s": "BTCUSDT",
            "u": 99,
            "b": "117415.500000000000000000",
            "B": "1.000000000000000000",
            "a": "117415.600000000000000000",
            "A": "2.000000000000000000",
        },
    )

    result = storage.persist_batch(lease, (mark, ticker))

    mark_part = next(part for part in result.normalized if "/mark_price/" in str(part.path))
    ticker_part = next(part for part in result.normalized if "/book_ticker/" in str(part.path))
    mark_table = pq.ParquetFile(mark_part.path).read()
    ticker_table = pq.ParquetFile(ticker_part.path).read()
    assert mark_table.schema.equals(MARK_PRICE_SCHEMA, check_metadata=True)
    assert ticker_table.schema.equals(BOOK_TICKER_SCHEMA, check_metadata=True)
    assert "provisional_funding_rate" in mark_table.column_names
    assert "funding_rate" not in mark_table.column_names
    assert not list(storage.data_root.rglob("funding_rate"))
    assert mark_table.column("mark_price").to_pylist() == [
        Decimal("117415.500000000000000000")
    ]
    assert ticker_table.column("bid_price").to_pylist() == [
        Decimal("117415.500000000000000000")
    ]
    lease.close()


def test_storage_api_accepts_no_caller_path_and_rejects_symlink_root(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    with pytest.raises(TypeError):
        storage.persist_batch(None, (kline(closed=True),), path="../../escape")  # type: ignore[call-arg]

    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    symlink = tmp_path / "linked"
    os.symlink(real, symlink)
    with pytest.raises(ValueError, match="root"):
        LiveStorage(symlink).acquire_writer("worker-a")
    assert not (tmp_path / "escape").exists()
