from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from re import compile as compile_regex
from types import MappingProxyType
from typing import Literal
from urllib.parse import parse_qsl, unquote, urlsplit
from uuid import UUID

from pydantic import Field, ValidationInfo, field_validator, model_validator

from crypto_research.contracts.base import UTCModel
from crypto_research.contracts.strategy import InstrumentRef

CONTRACT_SYMBOL_PATTERN = compile_regex(r"^[A-Z0-9]{3,32}$")
UNSIGNED_INTEGER_PATTERN = compile_regex(r"^(?:0|[1-9][0-9]*)$")
MAX_INT64 = 9_223_372_036_854_775_807
NUMERIC_REST_PARAMETERS = frozenset({"startTime", "endTime", "fromId", "limit"})
SENSITIVE_QUERY_PARAMETER_NAMES = frozenset(
    {"signature", "apikey", "api_key", "listenkey", "listen_key"}
)


@dataclass(frozen=True)
class RestEndpointSpec:
    allowed_parameters: frozenset[str]
    limit_maximum: int
    requires_one_minute_interval: bool = False
    from_id_excludes_times: bool = False
    maximum_time_range_ms: int | None = None


KLINE_REST_PARAMETERS = frozenset({"symbol", "interval", "startTime", "endTime", "limit"})
TIME_REST_PARAMETERS = frozenset({"symbol", "startTime", "endTime", "limit"})
REST_ENDPOINT_SPECS = MappingProxyType(
    {
        "/fapi/v1/klines": RestEndpointSpec(
            allowed_parameters=KLINE_REST_PARAMETERS,
            limit_maximum=1500,
            requires_one_minute_interval=True,
        ),
        "/fapi/v1/markPriceKlines": RestEndpointSpec(
            allowed_parameters=KLINE_REST_PARAMETERS,
            limit_maximum=1500,
            requires_one_minute_interval=True,
        ),
        "/fapi/v1/aggTrades": RestEndpointSpec(
            allowed_parameters=frozenset({*TIME_REST_PARAMETERS, "fromId"}),
            limit_maximum=1000,
            from_id_excludes_times=True,
            maximum_time_range_ms=3_600_000,
        ),
        "/fapi/v1/fundingRate": RestEndpointSpec(
            allowed_parameters=TIME_REST_PARAMETERS,
            limit_maximum=1000,
        ),
    }
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
    specification = REST_ENDPOINT_SPECS.get(parsed.path)
    if specification is None:
        raise ValueError("source URL must use an allowed public Binance REST endpoint")
    values = _parse_rest_query_parameters(parsed.query, specification)
    symbol = values.get("symbol")
    if symbol is None or CONTRACT_SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise ValueError("source URL must contain one uppercase contract symbol")
    if specification.requires_one_minute_interval and values.get("interval") != "1m":
        raise ValueError("source URL must contain interval=1m for kline data")
    _validate_rest_numeric_parameters(values, specification)


def _parse_rest_query_parameters(query: str, specification: RestEndpointSpec) -> dict[str, str]:
    try:
        parameters = parse_qsl(query, keep_blank_values=True, strict_parsing=True)
    except ValueError as error:
        raise ValueError("source URL must have valid REST query parameters") from error
    values: dict[str, str] = {}
    for key, value in parameters:
        if not key.strip() or not value.strip():
            raise ValueError("source URL cannot contain blank REST query keys or values")
        _reject_sensitive_query_parameter(key)
        if key not in specification.allowed_parameters:
            raise ValueError("source URL contains an unsupported REST query parameter")
        if key in values:
            raise ValueError("source URL cannot repeat REST query parameters")
        values[key] = value
    return values


def _validate_rest_numeric_parameters(
    values: dict[str, str], specification: RestEndpointSpec
) -> None:
    parsed_values = {
        key: _parse_unsigned_rest_integer(key, value)
        for key, value in values.items()
        if key in NUMERIC_REST_PARAMETERS
    }
    limit = parsed_values.get("limit")
    if limit is not None and not 1 <= limit <= specification.limit_maximum:
        raise ValueError("source URL contains an out-of-range REST limit")
    start_time = parsed_values.get("startTime")
    end_time = parsed_values.get("endTime")
    if start_time is not None and end_time is not None:
        if start_time > end_time:
            raise ValueError("source URL startTime cannot exceed endTime")
        if (
            specification.maximum_time_range_ms is not None
            and end_time - start_time > specification.maximum_time_range_ms
        ):
            raise ValueError("source URL REST time range exceeds the endpoint maximum")
    if specification.from_id_excludes_times and "fromId" in parsed_values and (
        start_time is not None or end_time is not None
    ):
        raise ValueError("source URL cannot mix aggTrades fromId with time parameters")


def _parse_unsigned_rest_integer(key: str, value: str) -> int:
    if UNSIGNED_INTEGER_PATTERN.fullmatch(value) is None:
        raise ValueError("source URL must use canonical unsigned REST integers")
    parsed_value = int(value)
    if key != "limit" and parsed_value > MAX_INT64:
        raise ValueError("source URL REST integer exceeds int64")
    return parsed_value


def _reject_sensitive_query_parameter(key: str) -> None:
    if key.casefold() in SENSITIVE_QUERY_PARAMETER_NAMES:
        raise ValueError("source URL cannot contain sensitive query parameters")


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
    streams = _parse_combined_websocket_streams(parsed.query)
    if any(not item for item in streams.split("/")):
        raise ValueError("source URL must carry non-empty stream identifiers")


def _parse_combined_websocket_streams(query: str) -> str:
    try:
        parameters = parse_qsl(query, keep_blank_values=True, strict_parsing=True)
    except ValueError as error:
        raise ValueError("source URL must have valid WebSocket query parameters") from error
    for key, _ in parameters:
        _reject_sensitive_query_parameter(key)
    if len(parameters) != 1:
        raise ValueError("source URL must contain exactly one WebSocket streams parameter")
    key, streams = parameters[0]
    if key != "streams" or not streams.strip():
        raise ValueError("source URL must contain one non-empty streams parameter")
    return streams
