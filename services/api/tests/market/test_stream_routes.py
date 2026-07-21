import pytest

from crypto_research.contracts.manifest import BinanceWebSocketSource
from crypto_research.market.binance.streams import (
    MAX_STREAMS_PER_CONNECTION,
    RoutedStream,
    StreamKind,
    StreamRoute,
    group_streams,
    streams_for_symbols,
)


def test_routed_sources_are_fixed_and_stream_names_are_lowercase() -> None:
    streams = streams_for_symbols(("BTCUSDT",))

    assert [(stream.kind, stream.route, stream.name) for stream in streams] == [
        (StreamKind.AGG_TRADE, StreamRoute.PUBLIC, "btcusdt@aggtrade"),
        (StreamKind.BOOK_TICKER, StreamRoute.PUBLIC, "btcusdt@bookticker"),
        (StreamKind.KLINE, StreamRoute.MARKET, "btcusdt@kline_1m"),
        (StreamKind.MARK_PRICE, StreamRoute.MARKET, "btcusdt@markprice@1s"),
    ]
    groups = group_streams(streams)
    assert all(isinstance(stream.source, BinanceWebSocketSource) for stream in streams)
    assert [group.uri for group in groups] == [
        "wss://fstream.binance.com/public/stream?streams="
        "btcusdt@aggtrade/btcusdt@bookticker",
        "wss://fstream.binance.com/market/stream?streams="
        "btcusdt@kline_1m/btcusdt@markprice@1s",
    ]


def test_stream_descriptor_rejects_unstructured_names_and_bad_symbols() -> None:
    with pytest.raises(TypeError):
        RoutedStream(  # type: ignore[call-arg]
            symbol="BTCUSDT",
            kind=StreamKind.KLINE,
            url="wss://attacker.invalid/stream",
        )
    with pytest.raises(ValueError, match="symbol"):
        RoutedStream(symbol="../BTCUSDT", kind=StreamKind.KLINE)


def test_grouping_never_exceeds_binance_stream_limit() -> None:
    streams = tuple(
        RoutedStream(symbol=f"S{index:04d}USDT", kind=StreamKind.AGG_TRADE)
        for index in range(MAX_STREAMS_PER_CONNECTION + 1)
    )

    groups = group_streams(streams)

    assert [len(group.streams) for group in groups] == [1024, 1]
    assert all(group.route is StreamRoute.PUBLIC for group in groups)
