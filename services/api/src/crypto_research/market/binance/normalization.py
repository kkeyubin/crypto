import csv
import io
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa

from crypto_research.market.binance.archive_paths import DatasetKind
from crypto_research.market.validation import ValidationError, parse_decimal, parse_millisecond


class NormalizationError(ValidationError):
    """A CSV member violates its declared Binance archive layout."""


class DuplicatePrimaryKeyError(NormalizationError):
    def __init__(self, duplicate_count: int) -> None:
        self.duplicate_count = duplicate_count
        super().__init__(f"duplicate primary keys: {duplicate_count}")


DECIMAL = pa.decimal128(38, 18)
KLINE_HEADERS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
)
FUNDING_HEADERS = ("calc_time", "funding_interval_hours", "last_funding_rate")
AGG_TRADE_HEADERS = (
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
)
KLINE_SCHEMA = pa.schema(
    [
        pa.field("open_time", pa.int64(), nullable=False),
        pa.field("open", DECIMAL, nullable=False),
        pa.field("high", DECIMAL, nullable=False),
        pa.field("low", DECIMAL, nullable=False),
        pa.field("close", DECIMAL, nullable=False),
        pa.field("volume", DECIMAL, nullable=False),
        pa.field("close_time", pa.int64(), nullable=False),
        pa.field("quote_asset_volume", DECIMAL, nullable=False),
        pa.field("number_of_trades", pa.int64(), nullable=False),
        pa.field("taker_buy_base_asset_volume", DECIMAL, nullable=False),
        pa.field("taker_buy_quote_asset_volume", DECIMAL, nullable=False),
    ]
)
FUNDING_SCHEMA = pa.schema(
    [
        pa.field("funding_time", pa.int64(), nullable=False),
        pa.field("funding_interval_hours", pa.int64(), nullable=False),
        pa.field("funding_rate", DECIMAL, nullable=False),
    ]
)
AGG_TRADE_SCHEMA = pa.schema(
    [
        pa.field("aggregate_trade_id", pa.int64(), nullable=False),
        pa.field("price", DECIMAL, nullable=False),
        pa.field("quantity", DECIMAL, nullable=False),
        pa.field("first_trade_id", pa.int64(), nullable=False),
        pa.field("last_trade_id", pa.int64(), nullable=False),
        pa.field("transact_time", pa.int64(), nullable=False),
        pa.field("is_buyer_maker", pa.bool_(), nullable=False),
    ]
)


@dataclass(frozen=True)
class NormalizedDataset:
    dataset: DatasetKind
    table: pa.Table
    primary_key_fields: tuple[str, ...]
    duplicates_removed: int = 0

    @property
    def row_count(self) -> int:
        return self.table.num_rows


def normalize_csv(
    dataset: DatasetKind, payload: bytes, start: datetime, end: datetime
) -> NormalizedDataset:
    """Parse official CSV rows directly into deterministic Arrow Decimal schemas."""
    start_ms, end_ms = _range_milliseconds(start, end)
    records = _read_rows(payload, _headers_for(dataset))
    if dataset in {DatasetKind.KLINES, DatasetKind.MARK_PRICE_KLINES}:
        rows = [_normalize_kline(row, start_ms, end_ms) for row in records]
        return _finish(dataset, rows, KLINE_SCHEMA, ("open_time",))
    if dataset is DatasetKind.FUNDING_RATE:
        rows = [_normalize_funding(row, start_ms, end_ms) for row in records]
        return _finish(dataset, rows, FUNDING_SCHEMA, ("funding_time",))
    if dataset is DatasetKind.AGG_TRADES:
        rows = [_normalize_aggregate_trade(row, start_ms, end_ms) for row in records]
        return _finish(dataset, rows, AGG_TRADE_SCHEMA, ("aggregate_trade_id",))
    raise NormalizationError(f"unsupported dataset: {dataset}")


def _read_rows(payload: bytes, headers: tuple[str, ...]) -> list[list[str]]:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise NormalizationError("CSV must be UTF-8") from error
    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        actual_headers = tuple(next(reader))
    except StopIteration as error:
        raise NormalizationError("CSV has no header") from error
    if actual_headers != headers:
        raise NormalizationError("unexpected CSV header")
    rows: list[list[str]] = []
    for row in reader:
        if not row:
            continue
        if len(row) != len(headers):
            raise NormalizationError("unexpected CSV column count")
        rows.append(row)
    if not rows:
        raise NormalizationError("CSV has no data rows")
    return rows


def _normalize_kline(row: list[str], start_ms: int, end_ms: int) -> dict[str, Any]:
    open_time = _timestamp_in_range(row[0], "open_time", start_ms, end_ms)
    close_time = parse_millisecond(row[6], field="close_time")
    if open_time % 60_000 != 0 or close_time != open_time + 59_999:
        raise NormalizationError("kline close_time must end its one-minute open_time interval")
    result = {
        "open_time": open_time,
        "open": _decimal(row[1], "open"),
        "high": _decimal(row[2], "high"),
        "low": _decimal(row[3], "low"),
        "close": _decimal(row[4], "close"),
        "volume": _decimal(row[5], "volume"),
        "close_time": close_time,
        "quote_asset_volume": _decimal(row[7], "quote_asset_volume"),
        "number_of_trades": _integer(row[8], "number_of_trades"),
        "taker_buy_base_asset_volume": _decimal(row[9], "taker_buy_base_asset_volume"),
        "taker_buy_quote_asset_volume": _decimal(row[10], "taker_buy_quote_asset_volume"),
    }
    return result


def _normalize_funding(row: list[str], start_ms: int, end_ms: int) -> dict[str, Any]:
    return {
        "funding_time": _timestamp_in_range(row[0], "funding_time", start_ms, end_ms),
        "funding_interval_hours": _integer(row[1], "funding_interval_hours"),
        "funding_rate": _decimal(row[2], "funding_rate"),
    }


def _normalize_aggregate_trade(row: list[str], start_ms: int, end_ms: int) -> dict[str, Any]:
    return {
        "aggregate_trade_id": _integer(row[0], "aggregate_trade_id"),
        "price": _decimal(row[1], "price"),
        "quantity": _decimal(row[2], "quantity"),
        "first_trade_id": _integer(row[3], "first_trade_id"),
        "last_trade_id": _integer(row[4], "last_trade_id"),
        "transact_time": _timestamp_in_range(row[5], "transact_time", start_ms, end_ms),
        "is_buyer_maker": _boolean(row[6]),
    }


def _finish(
    dataset: DatasetKind,
    rows: list[dict[str, Any]],
    schema: pa.Schema,
    primary_key_fields: tuple[str, ...],
) -> NormalizedDataset:
    primary_keys = [tuple(row[field] for field in primary_key_fields) for row in rows]
    duplicate_count = len(primary_keys) - len(set(primary_keys))
    if duplicate_count:
        raise DuplicatePrimaryKeyError(duplicate_count)
    if primary_keys != sorted(primary_keys):
        raise NormalizationError("rows must be sorted by primary key")
    try:
        table = pa.Table.from_pylist(rows, schema=schema)
    except (pa.ArrowException, ValueError) as error:
        raise NormalizationError("CSV values do not fit deterministic Arrow schema") from error
    return NormalizedDataset(dataset=dataset, table=table, primary_key_fields=primary_key_fields)


def _headers_for(dataset: DatasetKind) -> tuple[str, ...]:
    if dataset in {DatasetKind.KLINES, DatasetKind.MARK_PRICE_KLINES}:
        return KLINE_HEADERS
    if dataset is DatasetKind.FUNDING_RATE:
        return FUNDING_HEADERS
    if dataset is DatasetKind.AGG_TRADES:
        return AGG_TRADE_HEADERS
    raise NormalizationError(f"unsupported dataset: {dataset}")


def _range_milliseconds(start: datetime, end: datetime) -> tuple[int, int]:
    if start.tzinfo is None or end.tzinfo is None:
        raise NormalizationError("range timestamps must be UTC-aware")
    start = start.astimezone(UTC)
    end = end.astimezone(UTC)
    if end <= start:
        raise NormalizationError("range end must be after start")
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return _milliseconds_since_epoch(start, epoch), _milliseconds_since_epoch(end, epoch)


def _milliseconds_since_epoch(value: datetime, epoch: datetime) -> int:
    difference = value - epoch
    return (
        difference.days * 86_400_000
        + difference.seconds * 1_000
        + difference.microseconds // 1_000
    )


def _timestamp_in_range(value: str, field: str, start_ms: int, end_ms: int) -> int:
    parsed = parse_millisecond(value, field=field)
    if not start_ms <= parsed < end_ms:
        raise NormalizationError(f"{field} is outside requested UTC range")
    return parsed


def _decimal(value: str, field: str) -> Decimal:
    try:
        return parse_decimal(value, field=field)
    except ValidationError as error:
        raise NormalizationError(str(error)) from error


def _integer(value: str, field: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise NormalizationError(f"{field} must be an integer") from error


def _boolean(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise NormalizationError("is_buyer_maker must be true or false")
