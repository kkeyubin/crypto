from datetime import UTC, datetime
from enum import StrEnum
from re import compile as compile_regex
from typing import Literal
from urllib.parse import parse_qs, parse_qsl, unquote, urlsplit
from uuid import UUID

from pydantic import Field, ValidationInfo, field_validator, model_validator

from crypto_research.contracts.base import UTCModel
from crypto_research.contracts.strategy import InstrumentRef

CONTRACT_SYMBOL_PATTERN = compile_regex(r"^[A-Z0-9]{3,32}$")
REST_ENDPOINT_PARAMETERS = {
    "/fapi/v1/klines": frozenset({"symbol", "interval", "startTime", "endTime", "limit"}),
    "/fapi/v1/markPriceKlines": frozenset(
        {"symbol", "interval", "startTime", "endTime", "limit"}
    ),
    "/fapi/v1/aggTrades": frozenset({"symbol", "fromId", "startTime", "endTime", "limit"}),
    "/fapi/v1/fundingRate": frozenset({"symbol", "startTime", "endTime", "limit"}),
}
KLINE_REST_ENDPOINTS = frozenset({"/fapi/v1/klines", "/fapi/v1/markPriceKlines"})
SENSITIVE_REST_PARAMETER_NAMES = frozenset(
    {"signature", "apikey", "api_key", "listenkey", "listen_key"}
)


class DataType(StrEnum):
    KLINE_1M = "kline_1m"
    MARK_PRICE = "mark_price"
    FUNDING = "funding"
    AGG_TRADE = "agg_trade"
    BEST_BID_ASK = "best_bid_ask"


class SourceKind(StrEnum):
    BINANCE_ARCHIVE = "binance_archive"
    BINANCE_WEBSOCKET = "binance_websocket"
    BINANCE_REST = "binance_rest"


class ValidationState(StrEnum):
    PENDING = "pending"
    VALIDATED = "validated"
    REJECTED = "rejected"


class DeduplicationMethod(StrEnum):
    REJECT_DUPLICATES = "reject_duplicates"
    KEEP_FIRST = "keep_first"
    KEEP_LAST = "keep_last"


class RepairSource(StrEnum):
    BINANCE_ARCHIVE = "binance_archive"
    BINANCE_REST = "binance_rest"


class RepairResult(StrEnum):
    REPAIRED = "repaired"
    PARTIAL = "partial"
    FAILED = "failed"
    SOURCE_PENDING = "source_pending"


class MissingInterval(UTCModel):
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_time_range(self) -> "MissingInterval":
        if self.end <= self.start:
            raise ValueError("missing interval end must be after start")
        return self


class RepairRecord(UTCModel):
    started_at: datetime
    completed_at: datetime
    source: RepairSource
    result: RepairResult

    @model_validator(mode="after")
    def validate_time_range(self) -> "RepairRecord":
        if self.completed_at < self.started_at:
            raise ValueError("repair completion cannot precede start")
        return self


class DataManifest(UTCModel):
    manifest_id: UUID
    instrument: InstrumentRef
    data_type: DataType
    start: datetime
    end: datetime
    retrieved_at: datetime
    schema_version: Literal["2.0.0"]
    normalization_version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    source_kind: SourceKind
    source_object_url: str
    raw_path: str = Field(min_length=1)
    normalized_path: str = Field(min_length=1)
    source_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalized_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(gt=0)
    validation_state: ValidationState
    primary_key_fields: tuple[str, ...] = Field(min_length=1)
    deduplication_method: DeduplicationMethod
    duplicates_removed: int = Field(ge=0)
    missing_intervals: tuple[MissingInterval, ...] = ()
    repair_history: tuple[RepairRecord, ...] = ()

    @field_validator("raw_path", "normalized_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        if value.startswith("/") or ".." in value.split("/"):
            raise ValueError("manifest paths must be relative and cannot traverse parents")
        return value

    @field_validator("source_object_url")
    @classmethod
    def validate_source_object_url(cls, value: str, info: ValidationInfo) -> str:
        source_kind = info.data.get("source_kind")
        if not isinstance(source_kind, SourceKind):
            return value
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError as error:
            raise ValueError("source URL must have a valid port") from error
        if parsed.username is not None or parsed.password is not None or port is not None:
            raise ValueError("source URL cannot contain userinfo or a port")
        if parsed.fragment:
            raise ValueError("source URL cannot contain a fragment")
        if _contains_path_traversal(parsed.path):
            raise ValueError("source URL path cannot traverse parents")
        if source_kind is SourceKind.BINANCE_ARCHIVE:
            _validate_archive_url(parsed)
        elif source_kind is SourceKind.BINANCE_REST:
            _validate_rest_url(parsed)
        else:
            _validate_websocket_url(parsed)
        return value

    @field_validator("primary_key_fields")
    @classmethod
    def validate_primary_key_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not field for field in value) or len(set(value)) != len(value):
            raise ValueError("primary key fields must be non-empty and unique")
        return value

    @model_validator(mode="after")
    def validate_time_range(self) -> "DataManifest":
        if self.end <= self.start:
            raise ValueError("manifest end must be after start")
        if self.retrieved_at < self.end:
            raise ValueError("retrieval cannot precede dataset end")
        timestamps = (
            self.start,
            self.end,
            self.retrieved_at,
            *(record.started_at for record in self.repair_history),
            *(record.completed_at for record in self.repair_history),
        )
        if any(timestamp > datetime.now(UTC) for timestamp in timestamps):
            raise ValueError("timestamps cannot be in the future")
        if any(
            interval.start < self.start or interval.end > self.end
            for interval in self.missing_intervals
        ):
            raise ValueError("missing interval must be within manifest range")
        return self


def _contains_path_traversal(path: str) -> bool:
    decoded_path = path
    while True:
        unquoted_path = unquote(decoded_path)
        if unquoted_path == decoded_path:
            break
        decoded_path = unquoted_path
    return any(part in {".", ".."} for part in decoded_path.split("/"))


def _validate_archive_url(parsed: object) -> None:
    if not (
        parsed.scheme == "https"
        and parsed.netloc == "data.binance.vision"
        and parsed.path.startswith("/data/futures/um/")
        and parsed.path.endswith(".zip")
        and not parsed.query
    ):
        raise ValueError("source URL must be a canonical Binance archive ZIP URL")


def _validate_rest_url(parsed: object) -> None:
    if parsed.scheme != "https" or parsed.netloc != "fapi.binance.com":
        raise ValueError("source URL must be a canonical Binance REST URL")
    allowed_parameters = REST_ENDPOINT_PARAMETERS.get(parsed.path)
    if allowed_parameters is None:
        raise ValueError("source URL must use an allowed public Binance REST endpoint")
    try:
        parameters = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as error:
        raise ValueError("source URL must have valid REST query parameters") from error
    values: dict[str, str] = {}
    for key, value in parameters:
        if not key.strip() or not value.strip():
            raise ValueError("source URL cannot contain blank REST query keys or values")
        if key.casefold() in SENSITIVE_REST_PARAMETER_NAMES:
            raise ValueError("source URL cannot contain sensitive REST query parameters")
        if key not in allowed_parameters:
            raise ValueError("source URL contains an unsupported REST query parameter")
        if key in values:
            raise ValueError("source URL cannot repeat REST query parameters")
        values[key] = value
    symbol = values.get("symbol")
    if symbol is None or CONTRACT_SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise ValueError("source URL must contain one uppercase contract symbol")
    if parsed.path in KLINE_REST_ENDPOINTS and values.get("interval") != "1m":
        raise ValueError("source URL must contain interval=1m for kline data")


def _validate_websocket_url(parsed: object) -> None:
    if parsed.scheme != "wss" or parsed.netloc != "fstream.binance.com":
        raise ValueError("source URL must be a canonical Binance WebSocket URL")
    prefix = next(
        (candidate for candidate in ("/public/", "/market/") if parsed.path.startswith(candidate)),
        None,
    )
    if prefix is None:
        raise ValueError("source URL must use the public or market WebSocket path")
    endpoint = parsed.path.removeprefix(prefix)
    if endpoint.startswith("ws/"):
        stream_identifier = endpoint.removeprefix("ws/")
        if not stream_identifier or "/" in stream_identifier or parsed.query:
            raise ValueError("source URL must use a non-empty WebSocket stream identifier")
        return
    if endpoint != "stream":
        raise ValueError("source URL must use a WebSocket ws or stream endpoint")
    streams = parse_qs(parsed.query, keep_blank_values=True).get("streams", [])
    has_empty_stream = any(
        not stream or any(not item for item in stream.split("/")) for stream in streams
    )
    if not streams or has_empty_stream:
        raise ValueError("source URL must carry non-empty stream identifiers")
