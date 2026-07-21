import json
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from crypto_research.contracts import StrategySpecRecord
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
    StrategyMode,
    StrategySpec,
    StrategyState,
    VolmanRules,
    _canonical_content_hash,
)

DEFAULT_EXECUTION = ExecutionSpec()
DEFAULT_RISK = RiskSpec()
NON_FINITE_VALUES = [
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
]


def build_spec(
    *,
    mode: StrategyMode = StrategyMode.EXECUTABLE,
    family: StrategyFamily = StrategyFamily.BB,
    state: StrategyState = StrategyState.DRAFT,
    execution: ExecutionSpec | None = DEFAULT_EXECUTION,
    risk: RiskSpec | None = DEFAULT_RISK,
) -> StrategySpec:
    return StrategySpec(
        mode=mode,
        identity=StrategyIdentity(name="bb-btc-event", version="1.0.0", state=state),
        provenance=(
            ProvenanceRef(skill="volman-forex-price-action-scalping", section="ch10"),
            ProvenanceRef(
                skill="aronson-evidence-based-technical-analysis", section="ch06"
            ),
        ),
        instrument=InstrumentRef(
            venue="BINANCE", market="USD_M_PERPETUAL", symbol="BTCUSDT"
        ),
        bar=BarSpec(kind=BarKind.EVENT, trade_count=70),
        volman=VolmanRules(
            family=family,
            chronology=("box_known", "signal_line_frozen", "breakout"),
            frozen_signal_line="box_high_at_t",
            trigger="trade_price >= signal_line + breakout_bps",
            clear_path="target_distance_bps >= minimum_path_bps",
            invalidation="trade_price <= tipping_point",
        ),
        execution=execution,
        risk=risk,
        parameters=ParameterFamily(
            fixed={"side": "long"},
            search_space={
                "breakout_bps": (1.0, 2.0),
                "minimum_path_bps": (8.0, 12.0),
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


def test_strategy_input_dump_omits_computed_hash() -> None:
    spec = build_spec()

    assert spec.content_hash == build_spec().content_hash
    assert len(spec.content_hash) == 64
    assert "content_hash" not in spec.model_dump(mode="json")
    assert "content_hash" not in json.loads(spec.model_dump_json())


def test_strategy_input_rejects_caller_provided_hash() -> None:
    payload = build_spec().model_dump(mode="json", exclude={"content_hash"})

    with pytest.raises(ValidationError, match="content_hash"):
        StrategySpec.model_validate({**payload, "content_hash": "a" * 64})


def test_strategy_record_factory_computes_canonical_hash() -> None:
    spec = build_spec()

    record = StrategySpecRecord.from_spec(spec)

    assert record.content_hash == spec.content_hash
    assert record.model_dump(mode="json", exclude={"content_hash"}) == spec.model_dump(
        mode="json"
    )


def test_strategy_record_json_round_trip() -> None:
    record = StrategySpecRecord.from_spec(build_spec())

    restored = StrategySpecRecord.model_validate_json(record.model_dump_json())

    assert restored == record
    assert restored.model_dump_json() == record.model_dump_json()


@pytest.mark.parametrize(
    ("mode", "family", "execution", "risk"),
    [
        pytest.param(
            StrategyMode.EXECUTABLE,
            StrategyFamily.BB,
            DEFAULT_EXECUTION,
            DEFAULT_RISK,
            id="executable-bb",
        ),
        pytest.param(
            StrategyMode.EXECUTABLE,
            StrategyFamily.RB,
            DEFAULT_EXECUTION,
            DEFAULT_RISK,
            id="executable-rb",
        ),
        *[
            pytest.param(
                StrategyMode.OBSERVATION,
                family,
                None,
                None,
                id=f"observation-{family.value.lower()}",
            )
            for family in StrategyFamily
        ],
    ],
)
def test_every_valid_strategy_record_mode_family_round_trips_json(
    mode: StrategyMode,
    family: StrategyFamily,
    execution: ExecutionSpec | None,
    risk: RiskSpec | None,
) -> None:
    record = StrategySpecRecord.from_spec(
        build_spec(
            mode=mode,
            family=family,
            execution=execution,
            risk=risk,
        )
    )

    assert StrategySpecRecord.model_validate_json(record.model_dump_json()) == record


def test_strategy_record_rejects_tampered_hash() -> None:
    payload = StrategySpecRecord.from_spec(build_spec()).model_dump(mode="json")
    payload["content_hash"] = "0" * 64

    with pytest.raises(ValidationError, match="content_hash does not match"):
        StrategySpecRecord.model_validate_json(json.dumps(payload))


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


@pytest.mark.parametrize(
    ("venue", "market"),
    [("COINBASE", "USD_M_PERPETUAL"), ("BINANCE", "SPOT")],
)
def test_instrument_is_strictly_binance_usd_m_perpetual(
    venue: str, market: str
) -> None:
    with pytest.raises(ValidationError):
        InstrumentRef(venue=venue, market=market, symbol="BTCUSDT")


def test_instrument_accepts_exactly_one_symbol() -> None:
    with pytest.raises(ValidationError):
        InstrumentRef.model_validate(
            {
                "venue": "BINANCE",
                "market": "USD_M_PERPETUAL",
                "symbol": ["BTCUSDT", "ETHUSDT"],
            }
        )


@pytest.mark.parametrize(
    "family",
    [
        StrategyFamily.DD,
        StrategyFamily.FB,
        StrategyFamily.SB,
        StrategyFamily.IRB,
        StrategyFamily.ARB,
    ],
)
def test_executable_mode_rejects_observation_only_families(
    family: StrategyFamily,
) -> None:
    with pytest.raises(ValidationError, match="executable mode permits only BB or RB"):
        build_spec(family=family)


@pytest.mark.parametrize(
    ("execution", "risk"), [(None, RiskSpec()), (ExecutionSpec(), None)]
)
def test_executable_mode_requires_execution_and_risk(
    execution: ExecutionSpec | None, risk: RiskSpec | None
) -> None:
    with pytest.raises(ValidationError, match="requires execution and risk"):
        build_spec(execution=execution, risk=risk)


@pytest.mark.parametrize("family", list(StrategyFamily))
def test_observation_mode_accepts_all_families_without_executable_settings(
    family: StrategyFamily,
) -> None:
    spec = build_spec(
        mode=StrategyMode.OBSERVATION,
        family=family,
        execution=None,
        risk=None,
    )

    assert spec.volman.family is family


@pytest.mark.parametrize(
    ("execution", "risk"),
    [(ExecutionSpec(), None), (None, RiskSpec()), (ExecutionSpec(), RiskSpec())],
)
def test_observation_mode_rejects_execution_and_risk(
    execution: ExecutionSpec | None, risk: RiskSpec | None
) -> None:
    with pytest.raises(ValidationError, match="observation mode rejects execution and risk"):
        build_spec(
            mode=StrategyMode.OBSERVATION,
            execution=execution,
            risk=risk,
        )


def test_observation_mode_cannot_be_paper_enabled() -> None:
    with pytest.raises(ValidationError, match="observation mode cannot be paper_enabled"):
        build_spec(
            mode=StrategyMode.OBSERVATION,
            state=StrategyState.PAPER_ENABLED,
            execution=None,
            risk=None,
        )


def test_strategy_nested_collections_are_deeply_immutable() -> None:
    spec = build_spec()
    original_hash = spec.content_hash

    with pytest.raises(TypeError):
        dict.__setitem__(spec.parameters.fixed, "side", "short")
    with pytest.raises(TypeError):
        spec.parameters.fixed["side"] = "short"
    with pytest.raises(AttributeError):
        spec.parameters.fixed._FrozenMapping__items = (("side", "short"),)
    with pytest.raises(TypeError):
        spec.parameters.search_space["breakout_bps"] = (3.0,)
    with pytest.raises(AttributeError):
        spec.parameters.search_space["breakout_bps"].append(3.0)
    with pytest.raises(TypeError):
        spec.volman.chronology[0] = "mutated"

    assert isinstance(spec.provenance, tuple)
    assert isinstance(spec.nison_context, tuple)
    assert isinstance(spec.parameters.search_space["breakout_bps"], tuple)
    assert not hasattr(spec.parameters.fixed, "__dict__")
    assert isinstance(
        object.__getattribute__(spec.parameters.fixed, "_FrozenMapping__items"), tuple
    )
    assert spec.content_hash == original_hash


def test_parameter_mappings_keep_object_schema_and_json_serialization() -> None:
    spec = build_spec()
    parameter_schema = StrategySpec.model_json_schema()["$defs"]["ParameterFamily"]
    dumped_parameters = spec.model_dump(mode="json")["parameters"]

    assert parameter_schema["properties"]["fixed"]["type"] == "object"
    assert "additionalProperties" in parameter_schema["properties"]["fixed"]
    assert parameter_schema["properties"]["search_space"]["type"] == "object"
    assert "additionalProperties" in parameter_schema["properties"]["search_space"]
    assert dumped_parameters["fixed"] == {"side": "long"}
    assert isinstance(dumped_parameters["fixed"], dict)
    assert json.loads(spec.model_dump_json())["parameters"]["fixed"] == {"side": "long"}


def test_integer_contract_fields_reject_booleans() -> None:
    with pytest.raises(ValidationError):
        BarSpec(kind=BarKind.EVENT, trade_count=True)

    with pytest.raises(ValidationError):
        ExecutionSpec(latency_ms=True)


def test_risk_values_reject_numeric_strings() -> None:
    with pytest.raises(ValidationError):
        RiskSpec(risk_fraction="0.01")


@pytest.mark.parametrize("value", NON_FINITE_VALUES)
@pytest.mark.parametrize("parameter_field", ["fixed", "search_space"])
def test_strategy_parameters_reject_non_finite_values(
    value: float, parameter_field: str
) -> None:
    values: dict[str, object] = {
        "fixed": {"threshold": 1.0},
        "search_space": {"threshold": (1.0,)},
    }
    values[parameter_field] = (
        {"threshold": value}
        if parameter_field == "fixed"
        else {"threshold": (value,)}
    )

    with pytest.raises(ValidationError) as error:
        ParameterFamily.model_validate(values)

    assert "finite_number" in {item["type"] for item in error.value.errors()}


@pytest.mark.parametrize("value", NON_FINITE_VALUES)
def test_canonical_strategy_hash_rejects_non_finite_values(value: float) -> None:
    with pytest.raises(
        ValueError,
        match="Out of range float values are not JSON compliant",
    ):
        _canonical_content_hash({"threshold": value})


def test_evidence_plan_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        EvidencePlan(
            train_end=datetime(2024, 1, 1),
            validation_end=datetime(2024, 7, 1, tzinfo=UTC),
            test_end=datetime(2025, 1, 1, tzinfo=UTC),
            benchmark="permutation",
            multiple_testing="maximum statistic",
        )


def test_evidence_plan_rejects_non_utc_offsets() -> None:
    with pytest.raises(ValidationError, match=r"UTC offset \+00:00"):
        EvidencePlan(
            train_end=datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=8))),
            validation_end=datetime(2024, 7, 1, tzinfo=UTC),
            test_end=datetime(2025, 1, 1, tzinfo=UTC),
            benchmark="permutation",
            multiple_testing="maximum statistic",
        )
