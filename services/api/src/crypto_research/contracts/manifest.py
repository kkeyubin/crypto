from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Literal
from urllib.parse import urlencode
from uuid import UUID

from pydantic import Field, computed_field, field_validator, model_validator

from crypto_research.contracts.base import StrictFrozenModel, UTCModel
from crypto_research.contracts.strategy import InstrumentRef

ContractSymbol = Annotated[str, Field(pattern=r"^[A-Z0-9]{3,32}$")]
MAX_INT64 = 9_223_372_036_854_775_807


class DataType(StrEnum):
    KLINE_1M = "kline_1m"
    MARK_PRICE = "mark_price"
    FUNDING = "funding"
    AGG_TRADE = "agg_trade"
    BEST_BID_ASK = "best_bid_ask"


class ArchiveCadence(StrEnum):
    DAILY = "daily"
    MONTHLY = "monthly"


class ArchiveDataset(StrEnum):
    KLINES = "klines"
    MARK_PRICE_KLINES = "mark_price_klines"
    FUNDING_RATE = "funding_rate"
    AGG_TRADES = "agg_trades"


class BinanceRestEndpoint(StrEnum):
    EXCHANGE_INFO = "exchange_info"
    KLINES = "klines"
    MARK_PRICE_KLINES = "mark_price_klines"
    FUNDING_RATE = "funding_rate"
    AGG_TRADES = "agg_trades"


class BinanceStream(StrEnum):
    KLINE_1M = "kline_1m"
    MARK_PRICE = "mark_price"
    AGG_TRADE = "agg_trade"
    BEST_BID_ASK = "best_bid_ask"


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


@dataclass(frozen=True)
class ArchiveSourceSpec:
    path_component: str
    filename_component: str
    data_type: DataType
    requires_interval: bool


@dataclass(frozen=True)
class RestSourceSpec:
    path: str
    data_type: DataType | None
    parameter_order: tuple[str, ...]
    limit_maximum: int
    requires_interval: bool = False
    from_id_excludes_times: bool = False
    maximum_time_range_ms: int | None = None


@dataclass(frozen=True)
class WebSocketSourceSpec:
    route: str
    stream_suffix: str
    data_type: DataType


ARCHIVE_SOURCE_SPECS = MappingProxyType(
    {
        ArchiveDataset.KLINES: ArchiveSourceSpec("klines", "1m", DataType.KLINE_1M, True),
        ArchiveDataset.MARK_PRICE_KLINES: ArchiveSourceSpec(
            "markPriceKlines", "1m", DataType.MARK_PRICE, True
        ),
        ArchiveDataset.FUNDING_RATE: ArchiveSourceSpec(
            "fundingRate", "fundingRate", DataType.FUNDING, False
        ),
        ArchiveDataset.AGG_TRADES: ArchiveSourceSpec(
            "aggTrades", "aggTrades", DataType.AGG_TRADE, False
        ),
    }
)
REST_SOURCE_SPECS = MappingProxyType(
    {
        BinanceRestEndpoint.EXCHANGE_INFO: RestSourceSpec(
            "/fapi/v1/exchangeInfo",
            None,
            ("symbol",),
            1,
        ),
        BinanceRestEndpoint.KLINES: RestSourceSpec(
            "/fapi/v1/klines",
            DataType.KLINE_1M,
            ("symbol", "interval", "startTime", "endTime", "limit"),
            1500,
            requires_interval=True,
        ),
        BinanceRestEndpoint.MARK_PRICE_KLINES: RestSourceSpec(
            "/fapi/v1/markPriceKlines",
            DataType.MARK_PRICE,
            ("symbol", "interval", "startTime", "endTime", "limit"),
            1500,
            requires_interval=True,
        ),
        BinanceRestEndpoint.FUNDING_RATE: RestSourceSpec(
            "/fapi/v1/fundingRate",
            DataType.FUNDING,
            ("symbol", "startTime", "endTime", "limit"),
            1000,
        ),
        BinanceRestEndpoint.AGG_TRADES: RestSourceSpec(
            "/fapi/v1/aggTrades",
            DataType.AGG_TRADE,
            ("symbol", "fromId", "startTime", "endTime", "limit"),
            1000,
            from_id_excludes_times=True,
            maximum_time_range_ms=3_600_000,
        ),
    }
)
WEBSOCKET_SOURCE_SPECS = MappingProxyType(
    {
        BinanceStream.KLINE_1M: WebSocketSourceSpec("market", "kline_1m", DataType.KLINE_1M),
        BinanceStream.MARK_PRICE: WebSocketSourceSpec(
            "market", "markPrice@1s", DataType.MARK_PRICE
        ),
        BinanceStream.AGG_TRADE: WebSocketSourceSpec("market", "aggTrade", DataType.AGG_TRADE),
        BinanceStream.BEST_BID_ASK: WebSocketSourceSpec(
            "public", "bookTicker", DataType.BEST_BID_ASK
        ),
    }
)


class BinanceArchiveSource(UTCModel):
    kind: Literal["binance_archive"]
    cadence: ArchiveCadence
    dataset: ArchiveDataset
    symbol: ContractSymbol
    interval: Literal["1m"] | None = None
    period_start: datetime

    @model_validator(mode="after")
    def validate_archive_layout(self) -> "BinanceArchiveSource":
        time_parts = (
            self.period_start.hour,
            self.period_start.minute,
            self.period_start.second,
            self.period_start.microsecond,
        )
        if any(time_parts):
            raise ValueError("archive period_start must be midnight UTC")
        if self.cadence is ArchiveCadence.MONTHLY and self.period_start.day != 1:
            raise ValueError("monthly archive period_start must be the first day of the month")
        specification = ARCHIVE_SOURCE_SPECS[self.dataset]
        if specification.requires_interval != (self.interval == "1m"):
            raise ValueError("archive dataset interval requirement is not satisfied")
        return self

    @computed_field(return_type=str)
    @property
    def resolved_url(self) -> str:
        specification = ARCHIVE_SOURCE_SPECS[self.dataset]
        period_format = (
            "%Y-%m-%d" if self.cadence is ArchiveCadence.DAILY else "%Y-%m"
        )
        period = self.period_start.strftime(period_format)
        filename = f"{self.symbol}-{specification.filename_component}-{period}.zip"
        parts = [
            "https://data.binance.vision/data/futures/um",
            self.cadence,
            specification.path_component,
            self.symbol,
        ]
        if specification.requires_interval:
            parts.append("1m")
        parts.append(filename)
        return "/".join(parts)


class BinanceRestSource(StrictFrozenModel):
    kind: Literal["binance_rest"]
    endpoint: BinanceRestEndpoint
    symbol: ContractSymbol
    interval: Literal["1m"] | None = None
    start_time: int | None = None
    end_time: int | None = None
    from_id: int | None = None
    limit: int

    @model_validator(mode="after")
    def validate_rest_request(self) -> "BinanceRestSource":
        specification = REST_SOURCE_SPECS[self.endpoint]
        if specification.requires_interval != (self.interval == "1m"):
            raise ValueError("REST endpoint interval requirement is not satisfied")
        for value in (self.start_time, self.end_time, self.from_id):
            if value is not None and not 0 <= value <= MAX_INT64:
                raise ValueError("REST timestamps and from_id must be in the int64 range")
        if not 1 <= self.limit <= specification.limit_maximum:
            raise ValueError("REST limit is outside the endpoint range")
        if self.start_time is not None and self.end_time is not None:
            if self.start_time > self.end_time:
                raise ValueError("REST start_time cannot exceed end_time")
            if (
                specification.maximum_time_range_ms is not None
                and self.end_time - self.start_time > specification.maximum_time_range_ms
            ):
                raise ValueError("REST time range exceeds the endpoint maximum")
        if specification.from_id_excludes_times and self.from_id is not None and (
            self.start_time is not None or self.end_time is not None
        ):
            raise ValueError("aggTrades cannot mix from_id with time bounds")
        if not specification.from_id_excludes_times and self.from_id is not None:
            raise ValueError("REST endpoint does not support from_id")
        return self

    @computed_field(return_type=str)
    @property
    def resolved_url(self) -> str:
        specification = REST_SOURCE_SPECS[self.endpoint]
        values: dict[str, str | int | None] = {
            "symbol": self.symbol,
            "interval": self.interval,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "fromId": self.from_id,
            "limit": self.limit,
        }
        parameters = [
            (key, values[key])
            for key in specification.parameter_order
            if values[key] is not None
        ]
        return f"https://fapi.binance.com{specification.path}?{urlencode(parameters)}"


class BinanceWebSocketSource(StrictFrozenModel):
    kind: Literal["binance_websocket"]
    stream: BinanceStream
    symbol: ContractSymbol

    @computed_field(return_type=str)
    @property
    def resolved_url(self) -> str:
        specification = WEBSOCKET_SOURCE_SPECS[self.stream]
        return (
            f"wss://fstream.binance.com/{specification.route}/ws/"
            f"{self.symbol.lower()}@{specification.stream_suffix}"
        )


ManifestSource = Annotated[
    BinanceArchiveSource | BinanceRestSource | BinanceWebSocketSource,
    Field(discriminator="kind"),
]


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
    source: ManifestSource
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

    @field_validator("primary_key_fields")
    @classmethod
    def validate_primary_key_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not field for field in value) or len(set(value)) != len(value):
            raise ValueError("primary key fields must be non-empty and unique")
        return value

    @model_validator(mode="after")
    def validate_manifest(self) -> "DataManifest":
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
        if self.source.symbol != self.instrument.symbol:
            raise ValueError("manifest source symbol must match instrument symbol")
        if _source_data_type(self.source) is not self.data_type:
            raise ValueError("manifest source must match data_type")
        if self.start.microsecond % 1000 or self.end.microsecond % 1000:
            raise ValueError("manifest coverage boundaries must be millisecond-aligned")
        if isinstance(self.source, BinanceArchiveSource):
            expected_start = self.source.period_start
            expected_end = (
                expected_start + timedelta(days=1)
                if self.source.cadence is ArchiveCadence.DAILY
                else _next_month(expected_start)
            )
            if (self.start, self.end) != (expected_start, expected_end):
                raise ValueError("archive manifest coverage must match its source period")
        if isinstance(self.source, BinanceRestSource):
            start_ms = _epoch_milliseconds(self.start)
            end_ms = _epoch_milliseconds(self.end)
            if self.source.start_time is not None and self.source.start_time != start_ms:
                raise ValueError("REST manifest coverage must match source start_time")
            if self.source.end_time is not None and self.source.end_time != end_ms - 1:
                raise ValueError("REST manifest coverage must match half-open source end_time")
        return self


def _source_data_type(source: ManifestSource) -> DataType:
    if isinstance(source, BinanceArchiveSource):
        return ARCHIVE_SOURCE_SPECS[source.dataset].data_type
    if isinstance(source, BinanceRestSource):
        data_type = REST_SOURCE_SPECS[source.endpoint].data_type
        if data_type is None:
            raise ValueError("metadata REST sources cannot back a data manifest")
        return data_type
    return WEBSOCKET_SOURCE_SPECS[source.stream].data_type


def _next_month(value: datetime) -> datetime:
    if value.month == 12:
        return value.replace(year=value.year + 1, month=1)
    return value.replace(month=value.month + 1)


def _epoch_milliseconds(value: datetime) -> int:
    difference = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        difference.days * 86_400_000
        + difference.seconds * 1_000
        + difference.microseconds // 1_000
    )
