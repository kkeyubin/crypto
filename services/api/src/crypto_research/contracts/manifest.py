from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from crypto_research.contracts.base import UTCModel
from crypto_research.contracts.strategy import InstrumentRef


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
    source: str
    result: str

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
    schema_version: Literal["2.0.0"] = "2.0.0"
    normalization_version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    source_kind: SourceKind
    source_object_url: str = Field(pattern=r"^https://")
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
