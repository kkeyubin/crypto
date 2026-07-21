import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from crypto_research.contracts.ai import AIAssessment, AIOpinion, PrincipleCitation
from crypto_research.contracts.manifest import (
    DataManifest,
    DataType,
    DeduplicationMethod,
    MissingInterval,
    RepairRecord,
    RepairResult,
    RepairSource,
    SourceKind,
    ValidationState,
)
from crypto_research.contracts.market import (
    BestBidAsk,
    FundingObservation,
    MarketSnapshot,
    OHLCVBar,
)
from crypto_research.contracts.strategy import ExecutionSpec, InstrumentRef, RiskSpec

NOW = datetime(2025, 1, 1, tzinfo=UTC)
INSTRUMENT = InstrumentRef(venue="BINANCE", market="USD_M_PERPETUAL", symbol="PEPEUSDT")
NON_FINITE_VALUES = [
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
]


def manifest_fields() -> dict[str, object]:
    return {
        "source_kind": SourceKind.BINANCE_ARCHIVE,
        "source_object_url": "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/example.zip",
        "raw_path": "raw/binance/usdm/BTCUSDT/kline_1m/date=2025-01-01/source.zip",
        "normalized_path": "normalized/binance/usdm/BTCUSDT/kline_1m/date=2025-01-01/data.parquet",
        "source_checksum": "b" * 64,
        "normalized_checksum": "c" * 64,
        "row_count": 1,
        "validation_state": ValidationState.VALIDATED,
        "primary_key_fields": ("open_time",),
        "deduplication_method": DeduplicationMethod.REJECT_DUPLICATES,
        "duplicates_removed": 0,
    }


def manifest(**overrides: object) -> DataManifest:
    return DataManifest(
        manifest_id=uuid4(),
        instrument=INSTRUMENT,
        data_type=DataType.KLINE_1M,
        start=NOW,
        end=NOW + timedelta(minutes=1),
        retrieved_at=NOW + timedelta(minutes=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        **{**manifest_fields(), **overrides},
    )


@pytest.mark.parametrize(
    ("source_kind", "source_object_url"),
    [
        (
            SourceKind.BINANCE_ARCHIVE,
            "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/example.zip",
        ),
        (
            SourceKind.BINANCE_REST,
            "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m",
        ),
        (
            SourceKind.BINANCE_WEBSOCKET,
            "wss://fstream.binance.com/public/ws/btcusdt@kline_1m",
        ),
        (
            SourceKind.BINANCE_WEBSOCKET,
            "wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice@1s/pepeusdt@markPrice@1s",
        ),
    ],
)
def test_manifest_accepts_canonical_source_urls(
    source_kind: SourceKind, source_object_url: str
) -> None:
    data_manifest = manifest(source_kind=source_kind, source_object_url=source_object_url)

    assert data_manifest.source_object_url == source_object_url


@pytest.mark.parametrize(
    ("source_kind", "source_object_url"),
    [
        (SourceKind.BINANCE_ARCHIVE, "https://fapi.binance.com/fapi/v1/klines"),
        (SourceKind.BINANCE_REST, "https://data.binance.vision/data/futures/um/a.zip"),
        (SourceKind.BINANCE_ARCHIVE, "http://data.binance.vision/data/futures/um/a.zip"),
        (SourceKind.BINANCE_WEBSOCKET, "ws://fstream.binance.com/public/ws/btcusdt@aggTrade"),
        (SourceKind.BINANCE_ARCHIVE, "https://sub.data.binance.vision/data/futures/um/a.zip"),
        (SourceKind.BINANCE_REST, "https://fapi.binance.com:443/fapi/v1/klines"),
        (SourceKind.BINANCE_ARCHIVE, "https://data.binance.vision/data/futures/um/../a.zip"),
        (SourceKind.BINANCE_ARCHIVE, "https://data.binance.vision/data/futures/um/%2e%2e/a.zip"),
        (SourceKind.BINANCE_WEBSOCKET, "wss://fstream.binance.com/public/ws/"),
        (SourceKind.BINANCE_WEBSOCKET, "wss://fstream.binance.com/public/ws/btcusdt@aggTrade?x=1"),
        (SourceKind.BINANCE_WEBSOCKET, "wss://fstream.binance.com/market/stream"),
        (SourceKind.BINANCE_WEBSOCKET, "wss://fstream.binance.com/market/stream?streams="),
        (SourceKind.BINANCE_WEBSOCKET, "wss://fstream.binance.com/market/stream?streams=a//b"),
    ],
)
def test_manifest_rejects_noncanonical_or_cross_kind_source_urls(
    source_kind: SourceKind, source_object_url: str
) -> None:
    with pytest.raises(ValidationError, match="source URL"):
        manifest(source_kind=source_kind, source_object_url=source_object_url)


@pytest.mark.parametrize(
    "source_object_url",
    [
        "https://fapi.binance.com/fapi/v1/klines?limit=500&interval=1m&symbol=BTCUSDT",
        "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=PEPEUSDT&interval=1m",
        "https://fapi.binance.com/fapi/v1/aggTrades?symbol=BTCUSDT&fromId=1&limit=1000",
        "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=1&endTime=2",
    ],
)
def test_manifest_accepts_only_public_rest_dataset_endpoints(source_object_url: str) -> None:
    assert manifest(
        source_kind=SourceKind.BINANCE_REST, source_object_url=source_object_url
    ).source_object_url == source_object_url


@pytest.mark.parametrize(
    "source_object_url",
    [
        "https://fapi.binance.com/fapi/v1/order?symbol=BTCUSDT",
        "https://fapi.binance.com/fapi/v1/account?symbol=BTCUSDT",
        "https://fapi.binance.com/fapi/v1/listenKey?symbol=BTCUSDT",
        "https://fapi.binance.com/fapi/v1/exchangeInfo?symbol=BTCUSDT",
        "https://fapi.binance.com/fapi/v1/klines/?symbol=BTCUSDT&interval=1m",
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&orderId=1",
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&symbol=PEPEUSDT&interval=1m",
        "https://fapi.binance.com/fapi/v1/klines?symbol=&interval=1m",
        "https://fapi.binance.com/fapi/v1/klines?symbol=btcusdt&interval=1m",
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m",
        "https://fapi.binance.com/fapi/v1/klines?interval=1m",
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&signature=x",
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&APIKEY=x",
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&listen_key=x",
    ],
)
def test_manifest_rejects_private_or_malformed_rest_urls(source_object_url: str) -> None:
    with pytest.raises(ValidationError, match="source URL"):
        manifest(source_kind=SourceKind.BINANCE_REST, source_object_url=source_object_url)


@pytest.mark.parametrize(
    ("path", "query"),
    [
        (
            "/fapi/v1/klines",
            "symbol=BTCUSDT&interval=1m&startTime=0&endTime=9223372036854775807&limit=1500",
        ),
        (
            "/fapi/v1/markPriceKlines",
            "symbol=BTCUSDT&interval=1m&startTime=1&endTime=1&limit=1500",
        ),
        ("/fapi/v1/aggTrades", "symbol=BTCUSDT&startTime=0&endTime=3600000&limit=1000"),
        ("/fapi/v1/aggTrades", "symbol=BTCUSDT&fromId=0&limit=1"),
        ("/fapi/v1/fundingRate", "symbol=BTCUSDT&startTime=0&endTime=0&limit=1000"),
    ],
)
def test_manifest_accepts_bounded_canonical_rest_numeric_parameters(
    path: str, query: str
) -> None:
    source_object_url = f"https://fapi.binance.com{path}?{query}"

    assert manifest(
        source_kind=SourceKind.BINANCE_REST, source_object_url=source_object_url
    ).source_object_url == source_object_url


@pytest.mark.parametrize(
    ("path", "query"),
    [
        ("/fapi/v1/klines", "symbol=BTCUSDT&interval=1m&startTime=01"),
        ("/fapi/v1/klines", "symbol=BTCUSDT&interval=1m&startTime=-1"),
        ("/fapi/v1/klines", "symbol=BTCUSDT&interval=1m&startTime=1.0"),
        ("/fapi/v1/klines", "symbol=BTCUSDT&interval=1m&startTime=1e3"),
        (
            "/fapi/v1/klines",
            "symbol=BTCUSDT&interval=1m&startTime=9223372036854775808",
        ),
        ("/fapi/v1/klines", "symbol=BTCUSDT&interval=1m&limit=0"),
        ("/fapi/v1/klines", "symbol=BTCUSDT&interval=1m&limit=1501"),
        ("/fapi/v1/aggTrades", "symbol=BTCUSDT&limit=1001"),
        ("/fapi/v1/fundingRate", "symbol=BTCUSDT&limit=1001"),
        ("/fapi/v1/fundingRate", "symbol=BTCUSDT&startTime=2&endTime=1"),
        ("/fapi/v1/aggTrades", "symbol=BTCUSDT&fromId=1&startTime=1"),
        ("/fapi/v1/aggTrades", "symbol=BTCUSDT&fromId=1&endTime=1"),
        ("/fapi/v1/aggTrades", "symbol=BTCUSDT&startTime=0&endTime=3600001"),
    ],
)
def test_manifest_rejects_noncanonical_or_inconsistent_rest_numeric_parameters(
    path: str, query: str
) -> None:
    with pytest.raises(ValidationError, match="source URL"):
        manifest(
            source_kind=SourceKind.BINANCE_REST,
            source_object_url=f"https://fapi.binance.com{path}?{query}",
        )


@pytest.mark.parametrize(
    "source_object_url",
    [
        "wss://fstream.binance.com/public/stream?streams=btcusdt@aggTrade&apiKey=x",
        "wss://fstream.binance.com/public/stream?streams=btcusdt@aggTrade&signature=x",
        "wss://fstream.binance.com/public/stream?streams=btcusdt@aggTrade&listenKey=x",
        "wss://fstream.binance.com/public/stream?streams=btcusdt@aggTrade&streams=pepeusdt@aggTrade",
        "wss://fstream.binance.com/public/stream?streams=btcusdt@aggTrade&foo=x",
        "wss://fstream.binance.com/public/stream?Streams=btcusdt@aggTrade",
    ],
)
def test_manifest_rejects_noncanonical_combined_websocket_queries(source_object_url: str) -> None:
    with pytest.raises(ValidationError, match="source URL"):
        manifest(source_kind=SourceKind.BINANCE_WEBSOCKET, source_object_url=source_object_url)


def test_manifest_requires_explicit_v2_schema_version() -> None:
    payload = manifest().model_dump(mode="python")
    del payload["schema_version"]

    with pytest.raises(ValidationError):
        DataManifest.model_validate(payload)


def test_repair_record_uses_closed_source_and_result_enums() -> None:
    record = RepairRecord(
        started_at=NOW,
        completed_at=NOW,
        source=RepairSource.BINANCE_ARCHIVE,
        result=RepairResult.SOURCE_PENDING,
    )

    assert record.source is RepairSource.BINANCE_ARCHIVE
    assert record.result is RepairResult.SOURCE_PENDING


@pytest.mark.parametrize("field", ["source", "result"])
def test_repair_record_rejects_unknown_enum_values(field: str) -> None:
    payload: dict[str, object] = {
        "started_at": NOW,
        "completed_at": NOW,
        "source": RepairSource.BINANCE_ARCHIVE,
        "result": RepairResult.REPAIRED,
    }
    payload[field] = "unknown"

    with pytest.raises(ValidationError):
        RepairRecord(**payload)


def test_snapshot_rejects_bar_after_cutoff() -> None:
    with pytest.raises(ValidationError, match="after snapshot cutoff"):
        MarketSnapshot(
            snapshot_id=uuid4(),
            instrument=INSTRUMENT,
            cutoff=NOW,
            data_manifest_id=uuid4(),
            strategy_spec_hash="a" * 64,
            bars=(
                OHLCVBar(
                    timestamp=NOW + timedelta(minutes=1),
                    open=1,
                    high=1,
                    low=1,
                    close=1,
                    volume=1,
                ),
            ),
        )


def test_snapshot_rejects_best_bid_ask_after_cutoff() -> None:
    with pytest.raises(ValidationError, match="after snapshot cutoff"):
        MarketSnapshot(
            snapshot_id=uuid4(),
            instrument=INSTRUMENT,
            cutoff=NOW,
            data_manifest_id=uuid4(),
            strategy_spec_hash="a" * 64,
            bars=(),
            best_bid_ask=BestBidAsk(
                timestamp=NOW + timedelta(seconds=1), bid=1, ask=1
            ),
        )


def test_snapshot_rejects_funding_after_cutoff() -> None:
    with pytest.raises(ValidationError, match="after snapshot cutoff"):
        MarketSnapshot(
            snapshot_id=uuid4(),
            instrument=INSTRUMENT,
            cutoff=NOW,
            data_manifest_id=uuid4(),
            strategy_spec_hash="a" * 64,
            bars=(),
            funding=FundingObservation(
                timestamp=NOW + timedelta(seconds=1), rate=0.0001
            ),
        )


@pytest.mark.parametrize("timestamp", [NOW - timedelta(seconds=1), NOW])
def test_snapshot_accepts_and_serializes_funding_at_or_before_cutoff(
    timestamp: datetime,
) -> None:
    funding = FundingObservation(timestamp=timestamp, rate=-0.0001)
    snapshot = MarketSnapshot(
        snapshot_id=uuid4(),
        instrument=INSTRUMENT,
        cutoff=NOW,
        data_manifest_id=uuid4(),
        strategy_spec_hash="a" * 64,
        bars=(),
        funding=funding,
    )

    assert snapshot.funding == funding
    assert snapshot.model_dump(mode="json")["funding"] == funding.model_dump(
        mode="json"
    )


def test_funding_observation_is_strict_and_frozen() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        FundingObservation(timestamp=NOW, rate=0.0001, create_order=True)

    funding = FundingObservation(timestamp=NOW, rate=0.0001)
    with pytest.raises(ValidationError, match="frozen_instance"):
        funding.rate = 0.0002


@pytest.mark.parametrize("value", NON_FINITE_VALUES)
def test_funding_observation_rejects_non_finite_rate(value: float) -> None:
    with pytest.raises(ValidationError) as error:
        FundingObservation(timestamp=NOW, rate=value)

    assert "finite_number" in {item["type"] for item in error.value.errors()}


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


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
def test_ohlcv_rejects_numeric_strings(field: str) -> None:
    payload: dict[str, object] = {
        "timestamp": NOW,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 1.0,
    }
    payload[field] = "1.0"

    with pytest.raises(ValidationError):
        OHLCVBar.model_validate(payload)


def test_best_bid_ask_requires_non_negative_spread() -> None:
    with pytest.raises(ValidationError, match="ask must be"):
        BestBidAsk(timestamp=NOW, bid=2, ask=1)


@pytest.mark.parametrize("value", NON_FINITE_VALUES)
@pytest.mark.parametrize("financial_field", ["ohlcv_high", "maker_fee", "risk_fraction"])
def test_positive_financial_fields_reject_non_finite_values(
    value: float, financial_field: str
) -> None:
    with pytest.raises(ValidationError) as error:
        if financial_field == "ohlcv_high":
            OHLCVBar(
                timestamp=NOW,
                open=1,
                high=value,
                low=1,
                close=1,
                volume=1,
            )
        elif financial_field == "maker_fee":
            ExecutionSpec(maker_fee_bps=value)
        else:
            RiskSpec(risk_fraction=value)

    assert "finite_number" in {item["type"] for item in error.value.errors()}


def test_ai_schema_rejects_order_authority() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        AIAssessment(
            assessment_id=uuid4(),
            snapshot_id=uuid4(),
            opinion=AIOpinion.UNCERTAIN,
            reasons=("sample too small",),
            citations=(
                PrincipleCitation(
                    skill="aronson-evidence-based-technical-analysis", section="ch06"
                ),
            ),
            risk_notes=("cost sensitivity unknown",),
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
        reasons=("sample too small",),
        citations=(PrincipleCitation(skill="aronson", section="ch06"),),
        risk_notes=("cost sensitivity unknown",),
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
        bars=(OHLCVBar(timestamp=NOW, open=1, high=1, low=1, close=1, volume=1),),
    )

    with pytest.raises(AttributeError):
        assessment.reasons.append("mutated")
    with pytest.raises(TypeError):
        snapshot.bars[0] = snapshot.bars[0]

    assert isinstance(assessment.reasons, tuple)
    assert isinstance(assessment.citations, tuple)
    assert isinstance(assessment.risk_notes, tuple)
    assert isinstance(snapshot.bars, tuple)


def test_manifest_requires_source_sha256() -> None:
    with pytest.raises(ValidationError):
        DataManifest(
            manifest_id=uuid4(),
            instrument=INSTRUMENT,
            data_type=DataType.KLINE_1M,
            start=NOW,
            end=NOW + timedelta(minutes=1),
            retrieved_at=NOW,
            schema_version="2.0.0",
            normalization_version="1.0.0",
            **{**manifest_fields(), "source_checksum": "bad"},
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
            schema_version="2.0.0",
            normalization_version="1.0.0",
            **manifest_fields(),
        )


def test_manifest_collections_are_immutable() -> None:
    manifest = DataManifest(
        manifest_id=uuid4(),
        instrument=INSTRUMENT,
        data_type=DataType.KLINE_1M,
        start=NOW,
        end=NOW + timedelta(minutes=1),
        retrieved_at=NOW + timedelta(minutes=1),
        schema_version="2.0.0",
        normalization_version="1.0.0",
        **manifest_fields(),
        missing_intervals=(),
        repair_history=(),
    )

    with pytest.raises(AttributeError):
        manifest.missing_intervals.append(object())

    assert isinstance(manifest.missing_intervals, tuple)
    assert isinstance(manifest.repair_history, tuple)


def test_manifest_rejects_future_repair_records() -> None:
    future = datetime.now(UTC) + timedelta(minutes=5)

    with pytest.raises(ValidationError, match="timestamps cannot be in the future"):
        DataManifest(
            manifest_id=uuid4(),
            instrument=INSTRUMENT,
            data_type=DataType.KLINE_1M,
            start=NOW,
            end=NOW + timedelta(minutes=1),
            retrieved_at=NOW + timedelta(minutes=1),
            schema_version="2.0.0",
            normalization_version="1.0.0",
            **manifest_fields(),
            repair_history=(
                RepairRecord(
                    started_at=future,
                    completed_at=future,
                    source=RepairSource.BINANCE_ARCHIVE,
                    result=RepairResult.REPAIRED,
                ),
            ),
        )


def test_missing_interval_requires_positive_duration() -> None:
    with pytest.raises(ValidationError, match="missing interval end must be after start"):
        MissingInterval(start=NOW, end=NOW)


def test_repair_record_cannot_complete_before_it_starts() -> None:
    with pytest.raises(ValidationError, match="repair completion cannot precede start"):
        RepairRecord(
            started_at=NOW,
            completed_at=NOW - timedelta(seconds=1),
            source=RepairSource.BINANCE_ARCHIVE,
            result=RepairResult.REPAIRED,
        )


@pytest.mark.parametrize(
    ("missing_start", "missing_end"),
    [
        (NOW - timedelta(seconds=1), NOW + timedelta(seconds=1)),
        (NOW + timedelta(seconds=30), NOW + timedelta(minutes=1, seconds=1)),
    ],
)
def test_manifest_rejects_missing_interval_outside_coverage(
    missing_start: datetime, missing_end: datetime
) -> None:
    with pytest.raises(ValidationError, match="missing interval must be within manifest range"):
        DataManifest(
            manifest_id=uuid4(),
            instrument=INSTRUMENT,
            data_type=DataType.KLINE_1M,
            start=NOW,
            end=NOW + timedelta(minutes=1),
            retrieved_at=NOW + timedelta(minutes=1),
            schema_version="2.0.0",
            normalization_version="1.0.0",
            **manifest_fields(),
            missing_intervals=(MissingInterval(start=missing_start, end=missing_end),),
        )


def test_strict_contracts_parse_json_uuid_datetime_and_enum_strings() -> None:
    manifest_id = uuid4()
    payload = {
        "manifest_id": str(manifest_id),
        "instrument": {
            "venue": "BINANCE",
            "market": "USD_M_PERPETUAL",
            "symbol": "BTCUSDT",
        },
        "data_type": "kline_1m",
        "start": NOW.isoformat(),
        "end": (NOW + timedelta(minutes=1)).isoformat(),
        "retrieved_at": (NOW + timedelta(minutes=1)).isoformat(),
        "schema_version": "2.0.0",
        "normalization_version": "1.0.0",
        "source_kind": "binance_archive",
        "source_object_url": "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/example.zip",
        "raw_path": "raw/binance/usdm/BTCUSDT/kline_1m/date=2025-01-01/source.zip",
        "normalized_path": "normalized/binance/usdm/BTCUSDT/kline_1m/date=2025-01-01/data.parquet",
        "source_checksum": "b" * 64,
        "normalized_checksum": "c" * 64,
        "row_count": 1,
        "validation_state": "validated",
        "primary_key_fields": ["open_time"],
        "deduplication_method": "reject_duplicates",
        "duplicates_removed": 0,
    }

    manifest = DataManifest.model_validate_json(json.dumps(payload))

    assert manifest.manifest_id == manifest_id
    assert manifest.data_type is DataType.KLINE_1M
    assert manifest.start == NOW
