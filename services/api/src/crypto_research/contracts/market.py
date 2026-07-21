from datetime import datetime
from uuid import UUID

from pydantic import Field, model_validator

from crypto_research.contracts.base import UTCModel
from crypto_research.contracts.strategy import InstrumentRef


class OHLCVBar(UTCModel):
    timestamp: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_prices(self) -> "OHLCVBar":
        if self.high < max(self.open, self.close) or self.low > min(
            self.open, self.close
        ):
            raise ValueError("OHLC bounds do not contain open and close")
        if self.high < self.low:
            raise ValueError("high must be greater than or equal to low")
        return self


class BestBidAsk(UTCModel):
    timestamp: datetime
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_spread(self) -> "BestBidAsk":
        if self.ask < self.bid:
            raise ValueError("ask must be greater than or equal to bid")
        return self


class MarketSnapshot(UTCModel):
    schema_version: str = "1.0.0"
    snapshot_id: UUID
    instrument: InstrumentRef
    cutoff: datetime
    data_manifest_id: UUID
    strategy_spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    bars: tuple[OHLCVBar, ...]
    best_bid_ask: BestBidAsk | None = None
    funding_rate: float | None = None
    deterministic_signal_id: UUID | None = None

    @model_validator(mode="after")
    def prevent_future_observations(self) -> "MarketSnapshot":
        timestamps = [bar.timestamp for bar in self.bars]
        if self.best_bid_ask is not None:
            timestamps.append(self.best_bid_ask.timestamp)
        if any(timestamp > self.cutoff for timestamp in timestamps):
            raise ValueError("market observation occurs after snapshot cutoff")
        return self
