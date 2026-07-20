from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from crypto_research.contracts.strategy import (
    BarKind,
    BarSpec,
    EvidencePlan,
    ExecutionSpec,
    InstrumentRef,
    ParameterFamily,
    ProvenanceRef,
    RiskSpec,
    StrategyFamily,
    StrategyIdentity,
    StrategySpec,
    VolmanRules,
)


def build_spec() -> StrategySpec:
    return StrategySpec(
        identity=StrategyIdentity(name="bb-btc-event", version="1.0.0"),
        provenance=[
            ProvenanceRef(skill="volman-forex-price-action-scalping", section="ch10"),
            ProvenanceRef(
                skill="aronson-evidence-based-technical-analysis", section="ch06"
            ),
        ],
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol="BTCUSDT"
        ),
        bar=BarSpec(kind=BarKind.EVENT, trade_count=70),
        volman=VolmanRules(
            family=StrategyFamily.BB,
            chronology=["box_known", "signal_line_frozen", "breakout"],
            frozen_signal_line="box_high_at_t",
            trigger="trade_price >= signal_line + breakout_bps",
            clear_path="target_distance_bps >= minimum_path_bps",
            invalidation="trade_price <= tipping_point",
        ),
        execution=ExecutionSpec(),
        risk=RiskSpec(),
        parameters=ParameterFamily(
            fixed={"side": "long"},
            search_space={
                "breakout_bps": [1.0, 2.0],
                "minimum_path_bps": [8.0, 12.0],
            },
        ),
        evidence=EvidencePlan(
            train_end=datetime(2024, 1, 1, tzinfo=UTC),
            validation_end=datetime(2024, 7, 1, tzinfo=UTC),
            test_end=datetime(2025, 1, 1, tzinfo=UTC),
            benchmark="matched-position-bias permutation",
            multiple_testing="maximum-statistic permutation",
        ),
    )


def test_strategy_hash_is_stable() -> None:
    spec = build_spec()

    assert spec.content_hash == build_spec().content_hash
    assert len(spec.content_hash) == 64
    assert spec.model_dump(mode="json")["content_hash"] == spec.content_hash


def test_event_bar_requires_exactly_one_event_threshold() -> None:
    with pytest.raises(ValidationError, match="event bar requires exactly one"):
        BarSpec(kind=BarKind.EVENT, trade_count=70, volume=1000)


def test_evidence_windows_are_strictly_ordered() -> None:
    with pytest.raises(ValidationError, match="train < validation < test"):
        EvidencePlan(
            train_end=datetime(2025, 1, 1, tzinfo=UTC),
            validation_end=datetime(2024, 1, 1, tzinfo=UTC),
            test_end=datetime(2026, 1, 1, tzinfo=UTC),
            benchmark="permutation",
            multiple_testing="maximum statistic",
        )
