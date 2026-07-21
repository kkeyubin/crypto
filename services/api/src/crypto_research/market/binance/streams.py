from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from crypto_research.contracts.manifest import (
    MAX_INT64,
    BinanceStream,
    BinanceWebSocketSource,
)
from crypto_research.market.validation import normalize_symbol

MAX_STREAMS_PER_CONNECTION = 1024
_ROUTE_BASES = {
    "public": "wss://fstream.binance.com/public",
    "market": "wss://fstream.binance.com/market",
}


class StreamMessageError(ValueError):
    """A routed Binance message isn't a supported, internally consistent event."""


class StreamRoute(StrEnum):
    PUBLIC = "public"
    MARKET = "market"


class StreamKind(StrEnum):
    AGG_TRADE = "agg_trade"
    BOOK_TICKER = "book_ticker"
    KLINE = "kline"
    MARK_PRICE = "mark_price"


class LiveDataset(StrEnum):
    KLINES = "klines"
    MARK_PRICE = "mark_price"
    AGG_TRADES = "agg_trades"
    BOOK_TICKER = "book_ticker"


_KIND_ROUTE = {
    StreamKind.AGG_TRADE: StreamRoute.PUBLIC,
    StreamKind.BOOK_TICKER: StreamRoute.PUBLIC,
    StreamKind.KLINE: StreamRoute.MARKET,
    StreamKind.MARK_PRICE: StreamRoute.MARKET,
}
_KIND_SUFFIX = {
    StreamKind.AGG_TRADE: "aggtrade",
    StreamKind.BOOK_TICKER: "bookticker",
    StreamKind.KLINE: "kline_1m",
    StreamKind.MARK_PRICE: "markprice@1s",
}
_KIND_SOURCE = {
    StreamKind.AGG_TRADE: BinanceStream.AGG_TRADE,
    StreamKind.BOOK_TICKER: BinanceStream.BEST_BID_ASK,
    StreamKind.KLINE: BinanceStream.KLINE_1M,
    StreamKind.MARK_PRICE: BinanceStream.MARK_PRICE,
}


@dataclass(frozen=True)
class RoutedStream:
    symbol: str
    kind: StreamKind

    def __post_init__(self) -> None:
        try:
            normalized = normalize_symbol(self.symbol)
        except ValueError as error:
            raise ValueError("stream symbol is invalid") from error
        if not normalized.isalnum() or not 3 <= len(normalized) <= 32:
            raise ValueError("stream symbol is invalid")
        object.__setattr__(self, "symbol", normalized)

    @property
    def route(self) -> StreamRoute:
        return _KIND_ROUTE[self.kind]

    @property
    def name(self) -> str:
        return f"{self.symbol.lower()}@{_KIND_SUFFIX[self.kind]}"

    @property
    def source(self) -> BinanceWebSocketSource:
        return BinanceWebSocketSource(
            kind="binance_websocket",
            stream=_KIND_SOURCE[self.kind],
            symbol=self.symbol,
        )


@dataclass(frozen=True)
class StreamGroup:
    route: StreamRoute
    streams: tuple[RoutedStream, ...]

    def __post_init__(self) -> None:
        if not self.streams:
            raise ValueError("stream group cannot be empty")
        if len(self.streams) > MAX_STREAMS_PER_CONNECTION:
            raise ValueError("stream group exceeds Binance's 1024 stream limit")
        if any(stream.route is not self.route for stream in self.streams):
            raise ValueError("stream group cannot mix routed sources")
        if len({stream.name for stream in self.streams}) != len(self.streams):
            raise ValueError("stream group cannot contain duplicate streams")

    @property
    def uri(self) -> str:
        names = "/".join(stream.name for stream in self.streams)
        return f"{_ROUTE_BASES[self.route.value]}/stream?streams={names}"


@dataclass(frozen=True)
class ParsedStreamEvent:
    stream: RoutedStream
    venue: str
    market: str
    symbol: str
    dataset: LiveDataset
    source_event_time: int
    receive_time: datetime
    source_id: str | None
    values: Mapping[str, object]
    is_final: bool | None
    raw: Mapping[str, object]


def streams_for_symbols(symbols: Iterable[str]) -> tuple[RoutedStream, ...]:
    streams: list[RoutedStream] = []
    for symbol in sorted({normalize_symbol(value) for value in symbols}):
        streams.extend(RoutedStream(symbol, kind) for kind in StreamKind)
    return tuple(streams)


def group_streams(streams: Iterable[RoutedStream]) -> tuple[StreamGroup, ...]:
    unique = {stream.name: stream for stream in streams}
    groups: list[StreamGroup] = []
    for route in StreamRoute:
        routed = sorted(
            (stream for stream in unique.values() if stream.route is route),
            key=lambda stream: stream.name,
        )
        for start in range(0, len(routed), MAX_STREAMS_PER_CONNECTION):
            groups.append(StreamGroup(route, tuple(routed[start : start + 1024])))
    return tuple(groups)


def parse_stream_message(
    message: str | bytes | Mapping[str, object], receive_time: datetime
) -> ParsedStreamEvent:
    """Parse one supported Binance event without transport or storage side effects."""
    if receive_time.tzinfo is None or receive_time.utcoffset() != timedelta(0):
        raise StreamMessageError("receive_time must be UTC-aware")
    payload = _object(message)
    stream_name, event = _unwrap(payload)
    event_type = _text(event, "e")
    parsers = {
        "kline": _parse_kline,
        "markPriceUpdate": _parse_mark_price,
        "aggTrade": _parse_aggregate_trade,
        "bookTicker": _parse_book_ticker,
    }
    parser = parsers.get(event_type)
    if parser is None:
        raise StreamMessageError(f"unsupported Binance stream event: {event_type}")
    parsed = parser(event, receive_time)
    if stream_name is not None and (
        stream_name != stream_name.lower() or stream_name != parsed.stream.name
    ):
        raise StreamMessageError("combined stream identity does not match event")
    return parsed


def _object(message: str | bytes | Mapping[str, object]) -> Mapping[str, object]:
    if isinstance(message, Mapping):
        return message
    try:
        decoded = json.loads(message)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as error:
        raise StreamMessageError("stream message must be valid JSON") from error
    if not isinstance(decoded, dict):
        raise StreamMessageError("stream message must be a JSON object")
    return decoded


def _unwrap(payload: Mapping[str, object]) -> tuple[str | None, Mapping[str, object]]:
    if "stream" not in payload and "data" not in payload:
        return None, payload
    stream = payload.get("stream")
    data = payload.get("data")
    if not isinstance(stream, str) or not isinstance(data, dict):
        raise StreamMessageError("combined stream message requires stream and data")
    return stream, data


def _base(
    event: Mapping[str, object],
    receive_time: datetime,
    kind: StreamKind,
    dataset: LiveDataset,
    source_id: str | None,
    values: Mapping[str, object],
    *,
    is_final: bool | None = None,
) -> ParsedStreamEvent:
    symbol = _symbol(event.get("s"))
    return ParsedStreamEvent(
        stream=RoutedStream(symbol, kind),
        venue="BINANCE",
        market="USD_M_PERPETUAL",
        symbol=symbol,
        dataset=dataset,
        source_event_time=_unsigned_int64(event, "E"),
        receive_time=receive_time,
        source_id=source_id,
        values=values,
        is_final=is_final,
        raw=dict(event),
    )


def _parse_kline(
    event: Mapping[str, object], receive_time: datetime
) -> ParsedStreamEvent:
    raw_kline = event.get("k")
    if not isinstance(raw_kline, dict):
        raise StreamMessageError("kline event requires kline data")
    symbol = _symbol(event.get("s"))
    if _symbol(raw_kline.get("s")) != symbol or _text(raw_kline, "i") != "1m":
        raise StreamMessageError("kline identity or interval is invalid")
    open_time = _unsigned_int64(raw_kline, "t")
    close_time = _unsigned_int64(raw_kline, "T")
    if open_time % 60_000 or close_time != open_time + 59_999:
        raise StreamMessageError("kline timestamps must describe one complete minute")
    is_final = raw_kline.get("x")
    if not isinstance(is_final, bool):
        raise StreamMessageError("kline close state must be boolean")
    open_text, open_price = _decimal(raw_kline, "o", positive=True)
    high_text, high_price = _decimal(raw_kline, "h", positive=True)
    low_text, low_price = _decimal(raw_kline, "l", positive=True)
    close_text, close_price = _decimal(raw_kline, "c", positive=True)
    if low_price > min(open_price, close_price) or high_price < max(
        open_price, close_price
    ):
        raise StreamMessageError("kline OHLC values are inconsistent")
    values = {
        "open_time": open_time,
        "open": open_text,
        "high": high_text,
        "low": low_text,
        "close": close_text,
        "volume": _decimal_text(raw_kline, "v", nonnegative=True),
        "close_time": close_time,
        "quote_asset_volume": _decimal_text(raw_kline, "q", nonnegative=True),
        "number_of_trades": _unsigned_int64(raw_kline, "n"),
        "taker_buy_base_asset_volume": _decimal_text(
            raw_kline, "V", nonnegative=True
        ),
        "taker_buy_quote_asset_volume": _decimal_text(
            raw_kline, "Q", nonnegative=True
        ),
    }
    return _base(
        event,
        receive_time,
        StreamKind.KLINE,
        LiveDataset.KLINES,
        None,
        values,
        is_final=is_final,
    )


def _parse_mark_price(
    event: Mapping[str, object], receive_time: datetime
) -> ParsedStreamEvent:
    values = {
        "mark_price": _decimal_text(event, "p", positive=True),
        "index_price": _decimal_text(event, "i", positive=True),
        "estimated_settle_price": _decimal_text(event, "P", positive=True),
        "provisional_funding_rate": _decimal_text(event, "r"),
        "next_funding_time": _unsigned_int64(event, "T"),
    }
    return _base(
        event,
        receive_time,
        StreamKind.MARK_PRICE,
        LiveDataset.MARK_PRICE,
        None,
        values,
    )


def _parse_aggregate_trade(
    event: Mapping[str, object], receive_time: datetime
) -> ParsedStreamEvent:
    aggregate_trade_id = _unsigned_int64(event, "a")
    buyer_maker = event.get("m")
    if not isinstance(buyer_maker, bool):
        raise StreamMessageError("aggregate trade maker flag must be boolean")
    first_trade_id = _unsigned_int64(event, "f")
    last_trade_id = _unsigned_int64(event, "l")
    if first_trade_id > last_trade_id:
        raise StreamMessageError("aggregate trade ID range is invalid")
    values = {
        "aggregate_trade_id": aggregate_trade_id,
        "price": _decimal_text(event, "p", positive=True),
        "quantity": _decimal_text(event, "q", positive=True),
        "first_trade_id": first_trade_id,
        "last_trade_id": last_trade_id,
        "transact_time": _unsigned_int64(event, "T"),
        "is_buyer_maker": buyer_maker,
    }
    return _base(
        event,
        receive_time,
        StreamKind.AGG_TRADE,
        LiveDataset.AGG_TRADES,
        str(aggregate_trade_id),
        values,
    )


def _parse_book_ticker(
    event: Mapping[str, object], receive_time: datetime
) -> ParsedStreamEvent:
    update_id = _unsigned_int64(event, "u")
    bid_text, bid_price = _decimal(event, "b", positive=True)
    ask_text, ask_price = _decimal(event, "a", positive=True)
    if bid_price > ask_price:
        raise StreamMessageError("book ticker bid exceeds ask")
    values = {
        "update_id": update_id,
        "transact_time": _unsigned_int64(event, "T"),
        "bid_price": bid_text,
        "bid_quantity": _decimal_text(event, "B", nonnegative=True),
        "ask_price": ask_text,
        "ask_quantity": _decimal_text(event, "A", nonnegative=True),
    }
    return _base(
        event,
        receive_time,
        StreamKind.BOOK_TICKER,
        LiveDataset.BOOK_TICKER,
        str(update_id),
        values,
    )


def _symbol(value: object) -> str:
    if not isinstance(value, str):
        raise StreamMessageError("stream event symbol must be text")
    try:
        normalized = normalize_symbol(value)
    except ValueError as error:
        raise StreamMessageError("stream event symbol is invalid") from error
    if value != normalized or not normalized.isalnum() or not 3 <= len(normalized) <= 32:
        raise StreamMessageError("stream event symbol is invalid")
    return normalized


def _text(values: Mapping[str, Any], field: str) -> str:
    value = values.get(field)
    if not isinstance(value, str) or not value:
        raise StreamMessageError(f"{field} must be non-empty text")
    return value


def _unsigned_int64(values: Mapping[str, Any], field: str) -> int:
    value = values.get(field)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= MAX_INT64
    ):
        raise StreamMessageError(f"{field} must be an unsigned int64")
    return value


def _decimal_text(
    values: Mapping[str, Any],
    field: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> str:
    return _decimal(
        values, field, positive=positive, nonnegative=nonnegative
    )[0]


def _decimal(
    values: Mapping[str, Any],
    field: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> tuple[str, Decimal]:
    value = values.get(field)
    if not isinstance(value, str):
        raise StreamMessageError(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise StreamMessageError(f"{field} must be a decimal string") from error
    if not parsed.is_finite():
        raise StreamMessageError(f"{field} must be a finite decimal string")
    if not _fits_decimal128_38_18(parsed):
        raise StreamMessageError(f"{field} must fit decimal128(38, 18)")
    canonical = "0" if parsed.is_zero() else value
    if positive and parsed <= 0:
        raise StreamMessageError(f"{field} must be a positive decimal string")
    if nonnegative and parsed < 0:
        raise StreamMessageError(f"{field} must be a nonnegative decimal string")
    return canonical, parsed


def _fits_decimal128_38_18(value: Decimal) -> bool:
    """Return whether value is exactly representable at precision 38, scale 18."""
    _sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return True
    scale_shift = exponent + 18
    coefficient = digits
    if scale_shift < 0:
        removed = -scale_shift
        if removed > len(coefficient) or any(coefficient[-removed:]):
            return False
        coefficient = coefficient[:-removed]
        scale_shift = 0
    significant = tuple(coefficient)
    while significant and significant[0] == 0:
        significant = significant[1:]
    return len(significant) + scale_shift <= 38
