import json
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from crypto_research.market.binance.streams import (
    LiveDataset,
    StreamMessageError,
    parse_stream_message,
)

RECEIVED_AT = datetime(2026, 7, 21, 12, 0, 1, tzinfo=UTC)
MAX_INT64 = 9_223_372_036_854_775_807


def combined(stream: str, data: dict[str, object]) -> str:
    return json.dumps({"stream": stream, "data": data})


def valid_aggregate() -> dict[str, object]:
    return {
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
    }


def valid_book() -> dict[str, object]:
    return {
        "e": "bookTicker",
        "E": 1_753_099_200_011,
        "T": 1_753_099_200_010,
        "s": "BTCUSDT",
        "u": 99,
        "b": "1.24",
        "B": "1",
        "a": "1.25",
        "A": "2",
    }


def valid_kline() -> dict[str, object]:
    return {
        "e": "kline",
        "E": 1_753_099_200_999,
        "s": "BTCUSDT",
        "k": {
            "t": 1_753_099_200_000,
            "T": 1_753_099_259_999,
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
    }


def valid_mark() -> dict[str, object]:
    return {
        "e": "markPriceUpdate",
        "E": 1_753_099_200_999,
        "s": "BTCUSDT",
        "p": "117415.5",
        "i": "117400.1",
        "P": "117390",
        "r": "-0.0001",
        "T": 1_753_128_000_000,
    }


@pytest.mark.parametrize("closed", [False, True])
def test_kline_parser_preserves_exact_decimal_strings_and_close_state(closed: bool) -> None:
    message = combined(
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
                "o": "001.2300",
                "c": "1.2500",
                "h": "1.2600",
                "l": "1.2200",
                "v": "10.00000000",
                "n": 4,
                "x": closed,
                "q": "12.50000000",
                "V": "4.00000000",
                "Q": "5.00000000",
            },
        },
    )

    parsed = parse_stream_message(message, RECEIVED_AT)

    assert parsed.venue == "BINANCE"
    assert parsed.market == "USD_M_PERPETUAL"
    assert parsed.symbol == "BTCUSDT"
    assert parsed.dataset is LiveDataset.KLINES
    assert parsed.source_event_time == 1_753_099_200_999
    assert parsed.receive_time == RECEIVED_AT
    assert parsed.source_id is None
    assert parsed.is_final is closed
    assert parsed.values["open"] == "001.2300"
    assert parsed.values["quote_asset_volume"] == "12.50000000"


def test_mark_price_parser_retains_funding_fields() -> None:
    parsed = parse_stream_message(
        combined(
            "btcusdt@markprice@1s",
            {
                "e": "markPriceUpdate",
                "E": 1_753_099_200_999,
                "s": "BTCUSDT",
                "p": "117415.50000000",
                "i": "117400.10000000",
                "P": "117390.00000000",
                "r": "0.00010000",
                "T": 1_753_128_000_000,
            },
        ),
        RECEIVED_AT,
    )

    assert parsed.dataset is LiveDataset.MARK_PRICE
    assert parsed.source_id is None
    assert parsed.values == {
        "mark_price": "117415.50000000",
        "index_price": "117400.10000000",
        "estimated_settle_price": "117390.00000000",
        "provisional_funding_rate": "0.00010000",
        "next_funding_time": 1_753_128_000_000,
    }


def test_aggregate_trade_and_book_ticker_parsers_preserve_source_ids() -> None:
    aggregate = parse_stream_message(
        combined(
            "pepeusdt@aggtrade",
            {
                "e": "aggTrade",
                "E": 1_753_099_200_010,
                "s": "PEPEUSDT",
                "a": 42,
                "p": "0.0000123400",
                "q": "1000000.00000000",
                "f": 100,
                "l": 102,
                "T": 1_753_099_200_009,
                "m": True,
            },
        ),
        RECEIVED_AT,
    )
    ticker = parse_stream_message(
        combined(
            "pepeusdt@bookticker",
            {
                "e": "bookTicker",
                "E": 1_753_099_200_011,
                "T": 1_753_099_200_010,
                "s": "PEPEUSDT",
                "u": 99,
                "b": "0.0000123300",
                "B": "1000.00000000",
                "a": "0.0000123400",
                "A": "2000.00000000",
            },
        ),
        RECEIVED_AT,
    )

    assert aggregate.dataset is LiveDataset.AGG_TRADES
    assert aggregate.source_id == "42"
    assert aggregate.values["price"] == "0.0000123400"
    assert aggregate.values["is_buyer_maker"] is True
    assert ticker.dataset is LiveDataset.BOOK_TICKER
    assert ticker.source_id == "99"
    assert ticker.values["bid_price"] == "0.0000123300"


@pytest.mark.parametrize(
    "payload",
    [
        "not-json",
        json.dumps({"stream": "btcusdt@kline_1m"}),
        combined("btcusdt@unknown", {"e": "unknown", "E": 1, "s": "BTCUSDT"}),
        combined("btcusdt@aggtrade", {"e": "aggTrade", "E": 1, "s": "ETHUSDT"}),
        combined(
            "btcusdt@bookticker",
            {
                "e": "bookTicker",
                "E": 1,
                "T": 1,
                "s": "BTCUSDT",
                "u": 1,
                "b": "NaN",
                "B": "1",
                "a": "2",
                "A": "1",
            },
        ),
    ],
)
def test_malformed_or_unknown_messages_fail_closed(payload: str) -> None:
    with pytest.raises(StreamMessageError):
        parse_stream_message(payload, RECEIVED_AT)


def test_receive_time_must_be_utc_aware() -> None:
    with pytest.raises(StreamMessageError, match="UTC"):
        parse_stream_message(
            combined(
                "btcusdt@aggtrade",
                {
                    "e": "aggTrade",
                    "E": 1,
                    "s": "BTCUSDT",
                    "a": 1,
                    "p": "1",
                    "q": "1",
                    "f": 1,
                    "l": 1,
                    "T": 1,
                    "m": False,
                },
            ),
            datetime(2026, 7, 21),
        )


@pytest.mark.parametrize(
    ("stream", "event", "field", "invalid"),
    [
        ("btcusdt@aggtrade", valid_aggregate(), "E", -1),
        ("btcusdt@aggtrade", valid_aggregate(), "a", MAX_INT64 + 1),
        ("btcusdt@aggtrade", valid_aggregate(), "f", -1),
        ("btcusdt@aggtrade", valid_aggregate(), "T", MAX_INT64 + 1),
        ("btcusdt@bookticker", valid_book(), "u", -1),
        ("btcusdt@bookticker", valid_book(), "T", MAX_INT64 + 1),
        ("btcusdt@markprice@1s", valid_mark(), "T", -1),
    ],
)
def test_all_source_integer_fields_are_unsigned_int64(
    stream: str, event: dict[str, object], field: str, invalid: int
) -> None:
    payload = deepcopy(event)
    payload[field] = invalid

    with pytest.raises(StreamMessageError, match="unsigned int64"):
        parse_stream_message(combined(stream, payload), RECEIVED_AT)


@pytest.mark.parametrize(
    ("stream", "event", "field", "invalid"),
    [
        ("btcusdt@aggtrade", valid_aggregate(), "p", "0"),
        ("btcusdt@aggtrade", valid_aggregate(), "q", "0"),
        ("btcusdt@bookticker", valid_book(), "b", "-1"),
        ("btcusdt@bookticker", valid_book(), "B", "-1"),
        ("btcusdt@markprice@1s", valid_mark(), "p", "0"),
        ("btcusdt@markprice@1s", valid_mark(), "i", "-1"),
    ],
)
def test_price_and_quantity_fields_enforce_market_semantics(
    stream: str, event: dict[str, object], field: str, invalid: str
) -> None:
    payload = deepcopy(event)
    payload[field] = invalid

    with pytest.raises(StreamMessageError):
        parse_stream_message(combined(stream, payload), RECEIVED_AT)


def test_kline_integer_decimal_and_ohlc_relationships_fail_closed() -> None:
    invalid_events = []
    for field, value in (("t", -60_000), ("n", -1), ("v", "-1"), ("o", "0")):
        event = valid_kline()
        event["k"][field] = value  # type: ignore[index]
        invalid_events.append(event)
    inverted = valid_kline()
    inverted["k"]["h"] = "1.24"  # type: ignore[index]
    invalid_events.append(inverted)

    for event in invalid_events:
        with pytest.raises(StreamMessageError):
            parse_stream_message(combined("btcusdt@kline_1m", event), RECEIVED_AT)


def test_trade_id_order_and_uncrossed_book_are_required() -> None:
    trade = valid_aggregate()
    trade["f"] = 103
    trade["l"] = 102
    crossed = valid_book()
    crossed["b"] = "1.26"
    crossed["a"] = "1.25"

    with pytest.raises(StreamMessageError, match="trade ID"):
        parse_stream_message(combined("btcusdt@aggtrade", trade), RECEIVED_AT)
    with pytest.raises(StreamMessageError, match="bid"):
        parse_stream_message(combined("btcusdt@bookticker", crossed), RECEIVED_AT)


def test_negative_provisional_funding_rate_remains_valid() -> None:
    parsed = parse_stream_message(
        combined("btcusdt@markprice@1s", valid_mark()), RECEIVED_AT
    )

    assert parsed.values["provisional_funding_rate"] == "-0.0001"
