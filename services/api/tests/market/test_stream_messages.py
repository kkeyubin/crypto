import json
from datetime import UTC, datetime

import pytest

from crypto_research.market.binance.streams import (
    LiveDataset,
    StreamMessageError,
    parse_stream_message,
)

RECEIVED_AT = datetime(2026, 7, 21, 12, 0, 1, tzinfo=UTC)


def combined(stream: str, data: dict[str, object]) -> str:
    return json.dumps({"stream": stream, "data": data})


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
    assert parsed.source_id == "1753099200000"
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
    assert parsed.source_id == "1753099200999"
    assert parsed.values == {
        "mark_price": "117415.50000000",
        "index_price": "117400.10000000",
        "estimated_settle_price": "117390.00000000",
        "funding_rate": "0.00010000",
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
