import hashlib
import json
from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import Field, computed_field, model_validator

from crypto_research.contracts.base import StrictFrozenModel, UTCModel

ParameterValue = bool | int | float | str


class StrategyFamily(StrEnum):
    BB = "BB"
    RB = "RB"
    DD = "DD"
    FB = "FB"
    SB = "SB"
    IRB = "IRB"
    ARB = "ARB"


class StrategyState(StrEnum):
    DRAFT = "draft"
    REVIEWED = "reviewed"
    FROZEN = "frozen"
    BACKTESTED = "backtested"
    PAPER_ENABLED = "paper_enabled"
    RETIRED = "retired"


class EvidenceConclusion(StrEnum):
    CANDIDATE = "candidate"
    REJECTED = "rejected"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class BarKind(StrEnum):
    TIME = "time"
    EVENT = "event"


class StrategyIdentity(StrictFrozenModel):
    name: str = Field(pattern=r"^[a-z0-9-]+$")
    version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    state: StrategyState = StrategyState.DRAFT
    author: str = "owner"


class ProvenanceRef(StrictFrozenModel):
    skill: str
    section: str


class InstrumentRef(StrictFrozenModel):
    venue: str = Field(pattern=r"^[A-Z0-9_]+$")
    market: str = Field(pattern=r"^[A-Z0-9_]+$")
    symbol: str = Field(pattern=r"^[A-Z0-9]+$")


class BarSpec(StrictFrozenModel):
    kind: BarKind
    interval: str | None = None
    trade_count: int | None = Field(default=None, gt=0)
    volume: float | None = Field(default=None, gt=0)
    dollar_value: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_threshold(self) -> "BarSpec":
        event_values = [self.trade_count, self.volume, self.dollar_value]
        if self.kind is BarKind.TIME and (
            not self.interval or any(value is not None for value in event_values)
        ):
            raise ValueError("time bar requires interval and no event threshold")
        if self.kind is BarKind.EVENT and sum(
            value is not None for value in event_values
        ) != 1:
            raise ValueError("event bar requires exactly one event threshold")
        if self.kind is BarKind.EVENT and self.interval is not None:
            raise ValueError("event bar cannot define interval")
        return self


class NisonCondition(StrictFrozenModel):
    name: str
    expression: str
    source_section: str


class VolmanRules(StrictFrozenModel):
    family: StrategyFamily
    chronology: list[str] = Field(min_length=3)
    frozen_signal_line: str
    trigger: str
    clear_path: str
    invalidation: str


class ExecutionSpec(StrictFrozenModel):
    signal_source: str = "completed_bar"
    fill_timing: str = "next_executable_event"
    order_type: str = "stop_market"
    collision_policy: str = "event_order_or_conservative_stop_first"
    maker_fee_bps: float = Field(default=2.0, ge=0)
    taker_fee_bps: float = Field(default=5.0, ge=0)
    spread_bps: float = Field(default=1.0, ge=0)
    slippage_bps: float = Field(default=2.0, ge=0)
    latency_ms: int = Field(default=250, ge=0)
    funding_included: bool = True


class RiskSpec(StrictFrozenModel):
    risk_fraction: float = Field(default=0.005, gt=0, le=0.05)
    max_leverage: float = Field(default=1.0, ge=1, le=20)
    max_daily_loss_fraction: float = Field(default=0.02, gt=0, le=0.25)
    max_drawdown_fraction: float = Field(default=0.10, gt=0, le=0.50)
    stale_data_blocks_entries: bool = True


class ParameterFamily(StrictFrozenModel):
    fixed: dict[str, ParameterValue]
    search_space: dict[str, list[ParameterValue]]


class EvidencePlan(UTCModel):
    train_end: datetime
    validation_end: datetime
    test_end: datetime
    benchmark: str
    multiple_testing: str

    @model_validator(mode="after")
    def validate_order(self) -> "EvidencePlan":
        if not self.train_end < self.validation_end < self.test_end:
            raise ValueError("evidence windows must satisfy train < validation < test")
        return self


class StrategySpec(StrictFrozenModel):
    schema_version: str = "1.0.0"
    identity: StrategyIdentity
    provenance: Annotated[list[ProvenanceRef], Field(min_length=2)]
    instrument: InstrumentRef
    bar: BarSpec
    nison_context: list[NisonCondition] = Field(default_factory=list)
    volman: VolmanRules
    execution: ExecutionSpec
    risk: RiskSpec
    parameters: ParameterFamily
    evidence: EvidencePlan

    @computed_field
    @property
    def content_hash(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json", exclude={"content_hash"}),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()
