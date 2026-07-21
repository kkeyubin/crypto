import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AfterValidator, Field, PlainSerializer, model_validator

from crypto_research.contracts.base import FrozenMapping, StrictFrozenModel, UTCModel

ParameterValue = bool | int | float | str
FixedParameters = Annotated[
    Mapping[str, ParameterValue],
    AfterValidator(FrozenMapping),
    PlainSerializer(
        lambda value: dict(value.items()),
        return_type=dict[str, ParameterValue],
    ),
]
SearchSpace = Annotated[
    Mapping[str, tuple[ParameterValue, ...]],
    AfterValidator(FrozenMapping),
    PlainSerializer(
        lambda value: dict(value.items()),
        return_type=dict[str, tuple[ParameterValue, ...]],
    ),
]


class StrategyFamily(StrEnum):
    BB = "BB"
    RB = "RB"
    DD = "DD"
    FB = "FB"
    SB = "SB"
    IRB = "IRB"
    ARB = "ARB"


class StrategyMode(StrEnum):
    EXECUTABLE = "executable"
    OBSERVATION = "observation"


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
    venue: Literal["BINANCE"]
    market: Literal["USD_M_PERPETUAL"]
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
    chronology: tuple[str, ...] = Field(min_length=3)
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
    fixed: FixedParameters
    search_space: SearchSpace


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


class _StrategySpecPayload(StrictFrozenModel):
    schema_version: str = "1.0.0"
    mode: StrategyMode
    identity: StrategyIdentity
    provenance: Annotated[tuple[ProvenanceRef, ...], Field(min_length=2)]
    instrument: InstrumentRef
    bar: BarSpec
    nison_context: tuple[NisonCondition, ...] = ()
    volman: VolmanRules
    execution: ExecutionSpec | None = None
    risk: RiskSpec | None = None
    parameters: ParameterFamily
    evidence: EvidencePlan

    @model_validator(mode="after")
    def validate_mode_boundaries(self) -> "_StrategySpecPayload":
        if self.mode is StrategyMode.EXECUTABLE:
            if self.volman.family not in {StrategyFamily.BB, StrategyFamily.RB}:
                raise ValueError("executable mode permits only BB or RB")
            if self.execution is None or self.risk is None:
                raise ValueError("executable mode requires execution and risk")
        else:
            if self.execution is not None or self.risk is not None:
                raise ValueError("observation mode rejects execution and risk")
            if self.identity.state is StrategyState.PAPER_ENABLED:
                raise ValueError("observation mode cannot be paper_enabled")
        return self


def _canonical_content_hash(payload: Mapping[str, object]) -> str:
    canonical_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical_json).hexdigest()


class StrategySpec(_StrategySpecPayload):
    @property
    def content_hash(self) -> str:
        """Return the canonical payload hash without serializing it as input data."""
        return _canonical_content_hash(self.model_dump(mode="json"))


class StrategySpecRecord(_StrategySpecPayload):
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def from_spec(cls, spec: StrategySpec) -> "StrategySpecRecord":
        """Create the persisted/output form of an author input contract."""
        return cls.model_validate(
            {
                **spec.model_dump(mode="python"),
                "content_hash": spec.content_hash,
            }
        )

    @model_validator(mode="after")
    def validate_content_hash(self) -> "StrategySpecRecord":
        expected = _canonical_content_hash(
            self.model_dump(mode="json", exclude={"content_hash"})
        )
        if self.content_hash != expected:
            raise ValueError("content_hash does not match canonical strategy payload")
        return self
