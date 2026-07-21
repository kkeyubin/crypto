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


def test_routed_sources_match_binance_canonical_routes_and_stream_case() -> None:
    streams = streams_for_symbols(("BTCUSDT",))

    assert [(stream.kind, stream.route, stream.name) for stream in streams] == [
        (StreamKind.AGG_TRADE, StreamRoute.MARKET, "btcusdt@aggTrade"),
        (StreamKind.BOOK_TICKER, StreamRoute.PUBLIC, "btcusdt@bookTicker"),
        (StreamKind.KLINE, StreamRoute.MARKET, "btcusdt@kline_1m"),
        (StreamKind.MARK_PRICE, StreamRoute.MARKET, "btcusdt@markPrice@1s"),
    ]
    groups = group_streams(streams)
    assert all(isinstance(stream.source, BinanceWebSocketSource) for stream in streams)
    assert [group.uri for group in groups] == [
        "wss://fstream.binance.com/public/stream?streams="
        "btcusdt@bookTicker",
        "wss://fstream.binance.com/market/stream?streams="
        "btcusdt@aggTrade/btcusdt@kline_1m/btcusdt@markPrice@1s",
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
    assert all(group.route is StreamRoute.MARKET for group in groups)
