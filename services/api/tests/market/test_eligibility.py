from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from crypto_research.contracts.data import EligibilityReasonCode
from crypto_research.market.eligibility import (
    EligibilityContext,
    SymbolEligibilityPolicy,
    evaluate_eligibility,
)

NOW = datetime(2026, 7, 21, 12, tzinfo=UTC)


def eligible_context() -> EligibilityContext:
    return EligibilityContext(
        symbol="BTCUSDT",
        metadata_verified=True,
        history_start=NOW - timedelta(days=60),
        history_end=NOW,
        coverage_fraction=0.999,
        median_hourly_volume=1_000_000,
        live_last_event_at=NOW - timedelta(seconds=1),
        unrepaired_gap_count=0,
        data_ready=True,
        required_source_degraded=False,
    )


def policy() -> SymbolEligibilityPolicy:
    return SymbolEligibilityPolicy(
        symbol="BTCUSDT",
        minimum_history=timedelta(days=30),
        minimum_coverage_fraction=0.99,
        minimum_median_hourly_volume=100_000,
        live_stale_after=timedelta(seconds=30),
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"metadata_verified": False}, EligibilityReasonCode.METADATA_UNVERIFIED),
        (
            {"history_start": NOW - timedelta(days=2)},
            EligibilityReasonCode.INSUFFICIENT_HISTORY,
        ),
        ({"coverage_fraction": 0.8}, EligibilityReasonCode.INSUFFICIENT_COVERAGE),
        ({"median_hourly_volume": 1}, EligibilityReasonCode.INSUFFICIENT_LIQUIDITY),
        (
            {"live_last_event_at": NOW - timedelta(minutes=2)},
            EligibilityReasonCode.STALE_LIVE_DATA,
        ),
        ({"unrepaired_gap_count": 1}, EligibilityReasonCode.UNREPAIRED_GAP),
        ({"data_ready": False}, EligibilityReasonCode.DATA_NOT_READY),
        ({"required_source_degraded": True}, EligibilityReasonCode.SOURCE_DEGRADED),
    ],
)
def test_each_reason_independently_blocks_eligibility(
    changes: dict[str, object], reason: EligibilityReasonCode
) -> None:
    decision = evaluate_eligibility(
        replace(eligible_context(), **changes), policy(), now=NOW
    )

    assert decision.eligible is False
    assert decision.reason_codes == (reason,)


def test_all_reasons_are_evaluated_without_short_circuiting() -> None:
    context = EligibilityContext(
        symbol="1000PEPEUSDT",
        metadata_verified=False,
        history_start=NOW - timedelta(days=1),
        history_end=NOW,
        coverage_fraction=0.2,
        median_hourly_volume=1,
        live_last_event_at=None,
        unrepaired_gap_count=3,
        data_ready=False,
        required_source_degraded=True,
    )
    pepe_policy = SymbolEligibilityPolicy(
        symbol="1000PEPEUSDT",
        minimum_history=timedelta(days=30),
        minimum_coverage_fraction=0.99,
        minimum_median_hourly_volume=500_000,
        live_stale_after=timedelta(seconds=30),
    )

    decision = evaluate_eligibility(context, pepe_policy, now=NOW)

    assert set(decision.reason_codes) == set(EligibilityReasonCode)


def test_policy_must_be_owned_by_the_same_symbol() -> None:
    with pytest.raises(ValueError, match="symbol"):
        evaluate_eligibility(
            eligible_context(),
            replace(policy(), symbol="1000PEPEUSDT"),
            now=NOW,
        )
