import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
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


def aggregate_trade():
    return event(
        "pepeusdt@aggtrade",
        {
            "e": "aggTrade",
            "E": 1_753_099_200_010,
            "s": "PEPEUSDT",
            "a": 42,
            "p": "0.000012340000000000",
            "q": "1000000.000000000000000000",
            "f": 100,
            "l": 102,
            "T": 1_753_099_200_009,
            "m": True,
        },
    )


def test_closed_kline_is_published_raw_first_and_with_archive_schema(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")

    result = storage.persist(kline(closed=True))

    assert result.raw.path.relative_to(storage.data_root).as_posix() == (
        "raw/binance/usdm/BTCUSDT/klines/date=2025-07-21/live-events.parquet"
    )
    assert len(result.normalized) == 1
    normalized = result.normalized[0]
    assert normalized.path.relative_to(storage.data_root).as_posix() == (
        "normalized/binance/usdm/BTCUSDT/klines/date=2025-07-21/live.parquet"
    )
    table = pq.ParquetFile(normalized.path).read()
    assert table.schema.equals(KLINE_SCHEMA, check_metadata=True)
    assert table.column("open").to_pylist() == [Decimal("1.230000000000000000")]
    raw_table = pq.ParquetFile(result.raw.path).read()
    assert raw_table.num_rows == 1
    assert json.loads(raw_table.column("payload_json")[0].as_py())["e"] == "kline"


def test_in_progress_kline_is_retained_raw_but_not_normalized(tmp_path: Path) -> None:
    result = LiveStorage(tmp_path / "market-data").persist(kline(closed=False))

    assert result.raw.row_count == 1
    assert result.normalized == ()


def test_replaying_identical_event_is_idempotent(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    parsed = aggregate_trade()

    first = storage.persist(parsed)
    second = LiveStorage(storage.data_root).persist(parsed)

    assert first.raw.sha256 == second.raw.sha256
    assert first.normalized[0].sha256 == second.normalized[0].sha256
    assert second.raw.row_count == 1
    assert second.normalized[0].row_count == 1
    assert pq.ParquetFile(second.normalized[0].path).read().schema.equals(
        AGG_TRADE_SCHEMA, check_metadata=True
    )


def test_mark_price_funding_and_book_ticker_remain_decimal_exact(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
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

    mark_result = storage.persist(mark).normalized[0]
    ticker_result = storage.persist(ticker).normalized[0]

    mark_table = pq.ParquetFile(mark_result.path).read()
    ticker_table = pq.ParquetFile(ticker_result.path).read()
    assert mark_table.schema.equals(MARK_PRICE_SCHEMA, check_metadata=True)
    assert ticker_table.schema.equals(BOOK_TICKER_SCHEMA, check_metadata=True)
    assert mark_table.column("funding_rate").to_pylist() == [
        Decimal("0.000100000000000000")
    ]
    assert ticker_table.column("bid_price").to_pylist() == [
        Decimal("117415.500000000000000000")
    ]


def test_storage_api_accepts_no_caller_path_and_rejects_symlink_root(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    with pytest.raises(TypeError):
        storage.persist(kline(closed=True), path="../../escape")  # type: ignore[call-arg]

    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    symlink = tmp_path / "linked"
    os.symlink(real, symlink)
    with pytest.raises(ValueError, match="root"):
        LiveStorage(symlink).persist(kline(closed=True))
    assert not (tmp_path / "escape").exists()


def test_concurrent_distinct_events_do_not_lose_partition_rows(tmp_path: Path) -> None:
    storage = LiveStorage(tmp_path / "market-data")
    events = tuple(
        event(
            "pepeusdt@aggtrade",
            {
                "e": "aggTrade",
                "E": 1_753_099_200_010 + index,
                "s": "PEPEUSDT",
                "a": index,
                "p": "0.000012340000000000",
                "q": "1000000.000000000000000000",
                "f": index,
                "l": index,
                "T": 1_753_099_200_009 + index,
                "m": True,
            },
        )
        for index in range(24)
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(executor.map(storage.persist, events))

    normalized = (
        storage.data_root
        / "normalized/binance/usdm/PEPEUSDT/agg_trades/date=2025-07-21/live.parquet"
    )
    table = pq.ParquetFile(normalized).read()
    assert table.num_rows == len(events)
    assert table.column("aggregate_trade_id").to_pylist() == list(range(24))
