from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import Field, model_validator

from crypto_research.contracts.base import UTCModel
from crypto_research.contracts.strategy import InstrumentRef


class DataType(StrEnum):
    KLINE_1M = "kline_1m"
    MARK_PRICE = "mark_price"
    FUNDING = "funding"
    AGG_TRADE = "agg_trade"
    BEST_BID_ASK = "best_bid_ask"


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
    checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_version: str
    normalization_version: str
    missing_intervals: tuple[MissingInterval, ...] = ()
    repair_history: tuple[RepairRecord, ...] = ()

    @model_validator(mode="after")
    def validate_time_range(self) -> "DataManifest":
        if self.end <= self.start:
            raise ValueError("manifest end must be after start")
        if self.retrieved_at < self.end:
            raise ValueError("retrieval cannot precede dataset end")
        if any(
            interval.start < self.start or interval.end > self.end
            for interval in self.missing_intervals
        ):
            raise ValueError("missing interval must be within manifest range")
        return self
