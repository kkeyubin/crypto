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


class RepairRecord(UTCModel):
    started_at: datetime
    completed_at: datetime
    source: str
    result: str


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
        return self
