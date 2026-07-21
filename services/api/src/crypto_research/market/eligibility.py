from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from crypto_research.contracts.data import EligibilityReasonCode


@dataclass(frozen=True)
class SymbolEligibilityPolicy:
    symbol: str
    minimum_history: timedelta
    minimum_coverage_fraction: float
    minimum_median_hourly_volume: float
    live_stale_after: timedelta

    def __post_init__(self) -> None:
        if self.minimum_history <= timedelta(0):
            raise ValueError("minimum history must be positive")
        if not 0 <= self.minimum_coverage_fraction <= 1:
            raise ValueError("minimum coverage must be between zero and one")
        if self.minimum_median_hourly_volume < 0 or not math.isfinite(
            self.minimum_median_hourly_volume
        ):
            raise ValueError("minimum liquidity must be finite and non-negative")
        if self.live_stale_after <= timedelta(0):
            raise ValueError("live stale duration must be positive")


@dataclass(frozen=True)
class EligibilityContext:
    symbol: str
    metadata_verified: bool
    history_start: datetime
    history_end: datetime
    coverage_fraction: float
    median_hourly_volume: float
    live_last_event_at: datetime | None
    unrepaired_gap_count: int
    data_ready: bool
    required_source_degraded: bool


@dataclass(frozen=True)
class EligibilityDecision:
    symbol: str
    eligible: bool
    reason_codes: tuple[EligibilityReasonCode, ...]
    evaluated_at: datetime


def evaluate_eligibility(
    context: EligibilityContext,
    policy: SymbolEligibilityPolicy,
    *,
    now: datetime,
) -> EligibilityDecision:
    now = _utc(now, "now")
    symbol = context.symbol.strip().upper()
    if policy.symbol.strip().upper() != symbol:
        raise ValueError("eligibility policy symbol must match the evidence symbol")
    history_start = _utc(context.history_start, "history_start")
    history_end = _utc(context.history_end, "history_end")
    if history_end <= history_start:
        raise ValueError("history must be a non-empty half-open UTC range")
    if not 0 <= context.coverage_fraction <= 1 or not math.isfinite(
        context.coverage_fraction
    ):
        raise ValueError("coverage fraction must be finite and between zero and one")
    if context.median_hourly_volume < 0 or not math.isfinite(
        context.median_hourly_volume
    ):
        raise ValueError("liquidity metric must be finite and non-negative")
    if context.unrepaired_gap_count < 0:
        raise ValueError("gap count cannot be negative")
    last_event = (
        None
        if context.live_last_event_at is None
        else _utc(context.live_last_event_at, "live_last_event_at")
    )
    if last_event is not None and last_event > now:
        raise ValueError("live event timestamp cannot be in the future")

    checks = (
        (
            history_end - history_start < policy.minimum_history,
            EligibilityReasonCode.INSUFFICIENT_HISTORY,
        ),
        (
            last_event is None or now - last_event > policy.live_stale_after,
            EligibilityReasonCode.STALE_LIVE_DATA,
        ),
        (
            context.unrepaired_gap_count > 0,
            EligibilityReasonCode.UNREPAIRED_GAP,
        ),
        (
            context.median_hourly_volume < policy.minimum_median_hourly_volume,
            EligibilityReasonCode.INSUFFICIENT_LIQUIDITY,
        ),
        (
            not context.metadata_verified,
            EligibilityReasonCode.METADATA_UNVERIFIED,
        ),
        (
            context.coverage_fraction < policy.minimum_coverage_fraction,
            EligibilityReasonCode.INSUFFICIENT_COVERAGE,
        ),
        (not context.data_ready, EligibilityReasonCode.DATA_NOT_READY),
        (
            context.required_source_degraded,
            EligibilityReasonCode.SOURCE_DEGRADED,
        ),
    )
    reasons = tuple(reason for blocked, reason in checks if blocked)
    return EligibilityDecision(
        symbol=symbol,
        eligible=not reasons,
        reason_codes=reasons,
        evaluated_at=now,
    )


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field_name} must be UTC")
    return value.astimezone(UTC)
