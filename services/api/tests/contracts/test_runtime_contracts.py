from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from crypto_research.contracts.ai import AIAssessment, AIOpinion, PrincipleCitation
from crypto_research.contracts.manifest import DataManifest, DataType
from crypto_research.contracts.market import BestBidAsk, MarketSnapshot, OHLCVBar
from crypto_research.contracts.strategy import InstrumentRef

NOW = datetime(2025, 1, 1, tzinfo=UTC)
INSTRUMENT = InstrumentRef(venue="BINANCE", market="USD_M_PERPETUAL", symbol="PEPEUSDT")


def test_snapshot_rejects_bar_after_cutoff() -> None:
    with pytest.raises(ValidationError, match="after snapshot cutoff"):
        MarketSnapshot(
            snapshot_id=uuid4(),
            instrument=INSTRUMENT,
            cutoff=NOW,
            data_manifest_id=uuid4(),
            strategy_spec_hash="a" * 64,
            bars=[
                OHLCVBar(
                    timestamp=NOW + timedelta(minutes=1),
                    open=1,
                    high=1,
                    low=1,
                    close=1,
                    volume=1,
                )
            ],
        )


def test_snapshot_rejects_best_bid_ask_after_cutoff() -> None:
    with pytest.raises(ValidationError, match="after snapshot cutoff"):
        MarketSnapshot(
            snapshot_id=uuid4(),
            instrument=INSTRUMENT,
            cutoff=NOW,
            data_manifest_id=uuid4(),
            strategy_spec_hash="a" * 64,
            bars=[],
            best_bid_ask=BestBidAsk(
                timestamp=NOW + timedelta(seconds=1), bid=1, ask=1
            ),
        )


@pytest.mark.parametrize(
    ("bar", "message"),
    [
        (
            {"open": 2, "high": 1, "low": 1, "close": 1, "volume": 1},
            "OHLC bounds",
        ),
        (
            {"open": 1, "high": 1, "low": 2, "close": 1, "volume": 1},
            "OHLC bounds",
        ),
        (
            {"open": 1, "high": 1, "low": 2, "close": 3, "volume": 1},
            "OHLC bounds",
        ),
    ],
)
def test_ohlcv_requires_valid_price_bounds(
    bar: dict[str, int], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        OHLCVBar(timestamp=NOW, **bar)


def test_best_bid_ask_requires_non_negative_spread() -> None:
    with pytest.raises(ValidationError, match="ask must be"):
        BestBidAsk(timestamp=NOW, bid=2, ask=1)


def test_ai_schema_rejects_order_authority() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        AIAssessment(
            assessment_id=uuid4(),
            snapshot_id=uuid4(),
            opinion=AIOpinion.UNCERTAIN,
            reasons=["sample too small"],
            citations=[
                PrincipleCitation(
                    skill="aronson-evidence-based-technical-analysis", section="ch06"
                )
            ],
            risk_notes=["cost sensitivity unknown"],
            market_data_cutoff=NOW,
            model_id="model-x",
            prompt_version="1.0.0",
            skill_version="1.0.0",
            create_order=True,
        )


def test_runtime_contract_collections_are_immutable() -> None:
    assessment = AIAssessment(
        assessment_id=uuid4(),
        snapshot_id=uuid4(),
        opinion=AIOpinion.UNCERTAIN,
        reasons=["sample too small"],
        citations=[PrincipleCitation(skill="aronson", section="ch06")],
        risk_notes=["cost sensitivity unknown"],
        market_data_cutoff=NOW,
        model_id="model-x",
        prompt_version="1.0.0",
        skill_version="1.0.0",
    )
    snapshot = MarketSnapshot(
        snapshot_id=uuid4(),
        instrument=INSTRUMENT,
        cutoff=NOW,
        data_manifest_id=uuid4(),
        strategy_spec_hash="a" * 64,
        bars=[OHLCVBar(timestamp=NOW, open=1, high=1, low=1, close=1, volume=1)],
    )

    with pytest.raises(AttributeError):
        assessment.reasons.append("mutated")
    with pytest.raises(TypeError):
        snapshot.bars[0] = snapshot.bars[0]

    assert isinstance(assessment.reasons, tuple)
    assert isinstance(assessment.citations, tuple)
    assert isinstance(assessment.risk_notes, tuple)
    assert isinstance(snapshot.bars, tuple)


def test_manifest_requires_sha256() -> None:
    with pytest.raises(ValidationError):
        DataManifest(
            manifest_id=uuid4(),
            instrument=INSTRUMENT,
            data_type=DataType.KLINE_1M,
            start=NOW,
            end=NOW + timedelta(minutes=1),
            retrieved_at=NOW,
            checksum="bad",
            schema_version="1.0.0",
            normalization_version="1.0.0",
        )


@pytest.mark.parametrize(
    ("start", "end", "retrieved_at", "message"),
    [
        (NOW, NOW, NOW, "manifest end must be after start"),
        (
            NOW,
            NOW + timedelta(minutes=1),
            NOW,
            "retrieval cannot precede dataset end",
        ),
    ],
)
def test_manifest_validates_time_range(
    start: datetime, end: datetime, retrieved_at: datetime, message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        DataManifest(
            manifest_id=uuid4(),
            instrument=INSTRUMENT,
            data_type=DataType.KLINE_1M,
            start=start,
            end=end,
            retrieved_at=retrieved_at,
            checksum="a" * 64,
            schema_version="1.0.0",
            normalization_version="1.0.0",
        )


def test_manifest_collections_are_immutable() -> None:
    manifest = DataManifest(
        manifest_id=uuid4(),
        instrument=INSTRUMENT,
        data_type=DataType.KLINE_1M,
        start=NOW,
        end=NOW + timedelta(minutes=1),
        retrieved_at=NOW + timedelta(minutes=1),
        checksum="a" * 64,
        schema_version="1.0.0",
        normalization_version="1.0.0",
        missing_intervals=[],
        repair_history=[],
    )

    with pytest.raises(AttributeError):
        manifest.missing_intervals.append(object())

    assert isinstance(manifest.missing_intervals, tuple)
    assert isinstance(manifest.repair_history, tuple)
