import gzip
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
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
    LiveWriteResult,
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


def publish_all(storage: LiveStorage, lease, events) -> LiveWriteResult:
    replayed = 0
    for parsed in events:
        replayed += int(storage.accept(lease, parsed).replayed)
    raw = []
    normalized = []
    while result := storage.publish_next_batch(lease, max_events=256):
        raw.extend(result.raw)
        normalized.extend(result.normalized)
        assert result.batch_id is not None
        storage.acknowledge_cataloged(lease, result.batch_id)
    return LiveWriteResult(tuple(raw), tuple(normalized), replayed)


def test_batch_publishes_immutable_raw_ndjson_and_normalized_parquet_shards(
    tmp_path: Path,
) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")

    result = publish_all(storage, lease, (kline(closed=True), aggregate_trade()))

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

    result = publish_all(storage, lease, (kline(closed=False),))

    assert result.raw[0].row_count == 1
    assert result.normalized == ()
    lease.close()


def test_replay_identity_ignores_receive_time_but_conflicting_payload_fails(
    tmp_path: Path,
) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    parsed = aggregate_trade()
    first = publish_all(storage, lease, (parsed,))
    replay = replace(parsed, receive_time=parsed.receive_time + timedelta(seconds=30))

    second = storage.accept(lease, replay)

    assert second.replayed is True
    assert storage.publish_next_batch(lease, max_events=10) is None
    assert all(part.path.exists() for part in (*first.raw, *first.normalized))
    conflicting_raw = {**parsed.raw, "p": "0.000099990000000000"}
    conflicting = replace(parsed, raw=conflicting_raw)
    with pytest.raises(LiveStorageError, match="conflicts"):
        storage.accept(lease, conflicting)
    lease.close()


def test_same_identity_repeated_inside_one_batch_is_written_once(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    parsed = aggregate_trade()
    replay = replace(parsed, receive_time=parsed.receive_time + timedelta(seconds=1))

    result = publish_all(storage, lease, (parsed, replay))

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

    result = publish_all(storage, lease, events)

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

    result = publish_all(storage, lease, (mark, ticker))

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
        storage.accept(None, kline(closed=True), path="../../escape")  # type: ignore[call-arg]

    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    symlink = tmp_path / "linked"
    os.symlink(real, symlink)
    with pytest.raises(ValueError, match="root"):
        LiveStorage(symlink).acquire_writer("worker-a")
    assert not (tmp_path / "escape").exists()


def test_accept_is_durable_in_partitioned_sqlite_wal_before_publish(
    tmp_path: Path,
) -> None:
    root = tmp_path / "market-data"
    storage = LiveStorage(root)
    lease = storage.acquire_writer("worker-a")

    accepted = storage.accept(lease, aggregate_trade())

    assert accepted.accepted is True
    journals = list(root.glob("spool/binance/usdm/bucket=*/journal.sqlite3"))
    assert len(journals) == 1
    with sqlite3.connect(journals[0]) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute(
            "SELECT state, COUNT(*) FROM events GROUP BY state"
        ).fetchall() == [("queued", 1)]
    lease.close()

    restarted = LiveStorage(root)
    restarted_lease = restarted.acquire_writer("worker-b")
    result = restarted.publish_next_batch(restarted_lease, max_events=10)

    assert result is not None
    assert result.raw[0].row_count == 1
    assert result.normalized[0].row_count == 1
    restarted_lease.close()


def test_normalized_primary_key_is_unique_across_raw_shards(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    first = kline(closed=True)
    next_day = 86_400_000
    equivalent = replace(
        first,
        source_event_time=first.source_event_time + next_day,
        raw={**first.raw, "E": first.source_event_time + next_day},
    )

    storage.accept(lease, first)
    first_result = storage.publish_next_batch(lease, max_events=10)
    assert first_result is not None and first_result.batch_id is not None
    storage.acknowledge_cataloged(lease, first_result.batch_id)

    storage.accept(lease, equivalent)
    second_result = storage.publish_next_batch(lease, max_events=10)

    assert second_result is not None
    assert first_result.raw[0].path != second_result.raw[0].path
    assert first_result.normalized[0].row_count == 1
    assert second_result.raw[0].row_count == 1
    assert second_result.normalized == ()

    conflict = replace(
        first,
        source_event_time=first.source_event_time + 2 * next_day,
        values={**first.values, "close": "1.240000000000000000"},
        raw={
            **first.raw,
            "E": first.source_event_time + 2 * next_day,
            "k": {**first.raw["k"], "c": "1.240000000000000000"},  # type: ignore[dict-item]
        },
    )
    with pytest.raises(LiveStorageError, match="normalized primary key"):
        storage.accept(lease, conflict)
    lease.close()


def test_source_natural_key_conflicts_across_source_dates(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    first = aggregate_trade()
    storage.accept(lease, first)
    result = storage.publish_next_batch(lease, max_events=10)
    assert result is not None and result.batch_id is not None
    storage.acknowledge_cataloged(lease, result.batch_id)
    next_day = 86_400_000
    conflicting = replace(
        first,
        source_event_time=first.source_event_time + next_day,
        values={
            **first.values,
            "transact_time": int(first.values["transact_time"]) + next_day,
        },
        raw={
            **first.raw,
            "E": first.source_event_time + next_day,
            "T": int(first.raw["T"]) + next_day,
        },
    )

    with pytest.raises(LiveStorageError, match="source identity"):
        storage.accept(lease, conflicting)
    lease.close()


def test_fixed_bucket_journal_and_direct_ack_do_not_glob_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "market-data"
    storage = LiveStorage(root)
    lease = storage.acquire_writer("worker-a")
    storage.accept(lease, aggregate_trade())
    journals = list(root.glob("spool/binance/usdm/bucket=*/journal.sqlite3"))
    assert len(journals) == 1
    assert not list(root.glob("spool/binance/usdm/*/*/date=*/journal.sqlite3"))
    result = storage.publish_next_batch(lease, max_events=10)
    assert result is not None and result.batch_id is not None

    def reject_glob(*_args, **_kwargs):
        raise AssertionError("history glob is forbidden")

    monkeypatch.setattr(Path, "glob", reject_glob)
    storage.acknowledge_cataloged(lease, result.batch_id)
    lease.close()


@pytest.mark.parametrize("crash_on_publish", [1, 2])
def test_prepared_batch_recovers_with_deterministic_artifacts_after_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_on_publish: int,
) -> None:
    import crypto_research.market.live_storage as live_storage_module

    root = tmp_path / "market-data"
    storage = LiveStorage(root)
    lease = storage.acquire_writer("worker-a")
    storage.accept(lease, aggregate_trade())
    real_publish = live_storage_module._publish_immutable_bytes
    publish_calls = 0

    def crash_once(*args, **kwargs):
        nonlocal publish_calls
        publish_calls += 1
        if publish_calls == crash_on_publish:
            raise OSError("simulated process crash")
        return real_publish(*args, **kwargs)

    monkeypatch.setattr(live_storage_module, "_publish_immutable_bytes", crash_once)
    with pytest.raises(OSError, match="simulated process crash"):
        storage.publish_next_batch(lease, max_events=10)
    lease.close()
    monkeypatch.setattr(live_storage_module, "_publish_immutable_bytes", real_publish)

    restarted = LiveStorage(root)
    restarted_lease = restarted.acquire_writer("worker-b")
    recovered = restarted.publish_next_batch(restarted_lease, max_events=10)

    assert recovered is not None
    assert recovered.batch_id
    assert recovered.raw[0].path.name.startswith("part-")
    assert recovered.normalized[0].path.name.startswith("part-")
    assert restarted.publish_next_batch(restarted_lease, max_events=10) == recovered
    restarted.acknowledge_cataloged(restarted_lease, recovered.batch_id)
    assert restarted.publish_next_batch(restarted_lease, max_events=10) is None
    restarted_lease.close()


def test_prepared_batch_recovers_when_both_artifacts_precede_state_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from crypto_research.market.live_journal import LivePartitionJournal

    root = tmp_path / "market-data"
    storage = LiveStorage(root)
    lease = storage.acquire_writer("worker-a")
    storage.accept(lease, aggregate_trade())
    real_mark_published = LivePartitionJournal.mark_published
    failed = False

    def crash_before_state_commit(self, batch_id):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("simulated state-commit crash")
        return real_mark_published(self, batch_id)

    monkeypatch.setattr(
        LivePartitionJournal, "mark_published", crash_before_state_commit
    )
    with pytest.raises(OSError, match="state-commit crash"):
        storage.publish_next_batch(lease, max_events=10)
    assert len(list(root.rglob("part-*.ndjson.gz"))) == 1
    assert len(list(root.rglob("part-*.parquet"))) == 1
    lease.close()
    monkeypatch.setattr(
        LivePartitionJournal, "mark_published", real_mark_published
    )

    restarted = LiveStorage(root)
    restarted_lease = restarted.acquire_writer("worker-b")
    recovered = restarted.publish_next_batch(restarted_lease, max_events=10)

    assert recovered is not None and recovered.batch_id is not None
    assert len(list(root.rglob("part-*.ndjson.gz"))) == 1
    assert len(list(root.rglob("part-*.parquet"))) == 1
    restarted.acknowledge_cataloged(restarted_lease, recovered.batch_id)
    restarted_lease.close()


def test_lease_close_and_storage_mutation_share_one_fence(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    lease = storage.acquire_writer("worker-a")
    storage._write_lock.acquire()
    closed = threading.Event()

    def close_lease() -> None:
        lease.close()
        closed.set()

    closer = threading.Thread(target=close_lease)
    closer.start()
    time.sleep(0.05)
    assert closed.is_set() is False
    assert lease.active is True
    storage._write_lock.release()
    closer.join(timeout=1)

    assert closed.is_set() is True
    with pytest.raises(LiveWriterUnavailable, match="not active"):
        storage.accept(lease, aggregate_trade())
