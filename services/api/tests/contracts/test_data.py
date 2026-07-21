from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from crypto_research.contracts.data import (
    AddSymbolRequest,
    BackfillRequest,
    DataGapStatus,
    DataGapView,
    DataPartitionStatus,
    DataPartitionView,
    EligibilityReasonCode,
    EligibilityView,
    IngestionJobStatus,
    IngestionJobView,
    MarketDataHealthView,
    MetadataStatus,
    SourceMode,
    StreamStateView,
    StreamStatus,
    SymbolDataStatus,
    SymbolProfileView,
    SymbolView,
)
from crypto_research.contracts.manifest import (
    ArchiveCadence,
    ArchiveDataset,
    BinanceArchiveSource,
    DataManifest,
    DataType,
    DeduplicationMethod,
    ValidationState,
)
from crypto_research.contracts.strategy import InstrumentRef

NOW = datetime(2025, 1, 1, tzinfo=UTC)
START = NOW - timedelta(days=30)


def add_symbol_payload(**overrides: object) -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "history_start": START,
        "history_end": NOW,
        **overrides,
    }


def test_add_symbol_request_accepts_a_bounded_uppercase_utc_history() -> None:
    request = AddSymbolRequest(**add_symbol_payload(include_agg_trades=True))

    assert request.symbol == "BTCUSDT"
    assert request.include_agg_trades is True


@pytest.mark.parametrize("symbol", ["btcusdt", "BTC-USDT", "BTC USDT"])
def test_add_symbol_request_rejects_non_contract_symbols(symbol: str) -> None:
    with pytest.raises(ValidationError):
        AddSymbolRequest(**add_symbol_payload(symbol=symbol))


@pytest.mark.parametrize(
    "history_start,history_end,message",
    [
        (START, START, "history end must be after history start"),
        (START, START + timedelta(days=367), "history range cannot exceed 366 days"),
        (START.replace(tzinfo=None), NOW, "timestamps must be timezone-aware"),
        (
            START.astimezone(timezone(timedelta(hours=8))),
            NOW,
            "timestamps must use UTC offset",
        ),
        (
            datetime.now(UTC) - timedelta(days=1),
            datetime.now(UTC) + timedelta(minutes=5),
            "timestamps cannot be in the future",
        ),
    ],
)
def test_add_symbol_request_rejects_invalid_history_ranges(
    history_start: datetime, history_end: datetime, message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        AddSymbolRequest(
            **add_symbol_payload(history_start=history_start, history_end=history_end)
        )


def test_backfill_request_requires_explicit_aggregate_trade_history_opt_in() -> None:
    with pytest.raises(ValidationError, match="aggregate-trade history requires explicit opt-in"):
        BackfillRequest(
            symbol="BTCUSDT",
            data_types=(DataType.AGG_TRADE,),
            start=START,
            end=NOW,
        )


def test_backfill_request_accepts_explicit_aggregate_trade_history_opt_in() -> None:
    request = BackfillRequest(
        symbol="BTCUSDT",
        data_types=(DataType.KLINE_1M, DataType.AGG_TRADE),
        start=START,
        end=NOW,
        include_agg_trades=True,
    )

    assert request.data_types == (DataType.KLINE_1M, DataType.AGG_TRADE)


def test_symbol_view_exposes_independent_data_and_metadata_statuses() -> None:
    view = SymbolView(
        symbol="PEPEUSDT",
        enabled=True,
        history_start=START,
        history_end=NOW,
        include_agg_trades=False,
        data_status=SymbolDataStatus.DATA_READY,
        metadata_status=MetadataStatus.METADATA_UNVERIFIED,
        created_at=START,
        updated_at=NOW,
    )

    assert view.data_status is SymbolDataStatus.DATA_READY
    assert view.metadata_status is MetadataStatus.METADATA_UNVERIFIED


def test_ingestion_job_view_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        IngestionJobView(
            job_id=uuid4(),
            symbol="BTCUSDT",
            data_type=DataType.KLINE_1M,
            status=IngestionJobStatus.QUEUED,
            requested_start=START,
            requested_end=NOW,
            created_at=START,
            updated_at=NOW,
            create_order=True,
        )


def test_data_partition_view_rejects_impossible_counts() -> None:
    with pytest.raises(ValidationError) as error:
        DataPartitionView(
            partition_id=uuid4(),
            symbol="BTCUSDT",
            data_type=DataType.KLINE_1M,
            start=START,
            end=NOW,
            parquet_path="normalized/binance/usdm/BTCUSDT/kline_1m/date=2024-12-02/data.parquet",
            checksum="a" * 64,
            row_count=0,
            version=0,
            status=DataPartitionStatus.APPROVED,
            created_at=NOW,
        )

    assert {item["loc"] for item in error.value.errors()} == {("row_count",), ("version",)}


@pytest.mark.parametrize("field", ["row_count", "version"])
def test_data_partition_view_rejects_numeric_strings(field: str) -> None:
    payload: dict[str, object] = {
        "partition_id": uuid4(),
        "symbol": "BTCUSDT",
        "data_type": DataType.KLINE_1M,
        "start": START,
        "end": NOW,
        "parquet_path": "normalized/binance/usdm/BTCUSDT/kline_1m/date=2024-12-02/data.parquet",
        "checksum": "a" * 64,
        "row_count": 1,
        "version": 1,
        "status": DataPartitionStatus.APPROVED,
        "created_at": NOW,
    }
    payload[field] = "1"

    with pytest.raises(ValidationError) as error:
        DataPartitionView(**payload)

    assert {item["loc"] for item in error.value.errors()} == {(field,)}


@pytest.mark.parametrize("field", ["row_count", "duplicates_removed"])
def test_data_manifest_rejects_numeric_strings(field: str) -> None:
    payload: dict[str, object] = {
        "manifest_id": uuid4(),
        "instrument": InstrumentRef(venue="BINANCE", market="USD_M_PERPETUAL", symbol="BTCUSDT"),
        "data_type": DataType.KLINE_1M,
        "start": START,
        "end": NOW,
        "retrieved_at": NOW,
        "schema_version": "2.0.0",
        "normalization_version": "1.0.0",
        "source": BinanceArchiveSource(
            kind="binance_archive",
            cadence=ArchiveCadence.DAILY,
            dataset=ArchiveDataset.KLINES,
            symbol="BTCUSDT",
            interval="1m",
            period_start=NOW,
        ),
        "raw_path": "raw/binance/usdm/BTCUSDT/kline_1m/date=2024-12-02/source.zip",
        "normalized_path": "normalized/binance/usdm/BTCUSDT/kline_1m/date=2024-12-02/data.parquet",
        "source_checksum": "b" * 64,
        "normalized_checksum": "c" * 64,
        "row_count": 1,
        "validation_state": ValidationState.VALIDATED,
        "primary_key_fields": ("open_time",),
        "deduplication_method": DeduplicationMethod.REJECT_DUPLICATES,
        "duplicates_removed": 0,
    }
    payload[field] = "1"

    with pytest.raises(ValidationError) as error:
        DataManifest(**payload)

    assert {item["loc"] for item in error.value.errors()} == {(field,)}


@pytest.mark.parametrize(
    "field",
    [
        "sample_count",
        "coverage_fraction",
        "realized_volatility",
        "jump_frequency",
        "median_spread_bps",
        "median_hourly_volume",
        "funding_rate_mean",
    ],
)
def test_symbol_profile_view_rejects_numeric_strings(field: str) -> None:
    payload: dict[str, object] = {
        "symbol": "BTCUSDT",
        "calculated_at": NOW,
        "coverage_start": START,
        "coverage_end": NOW,
        "sample_count": 1,
        "coverage_fraction": 1.0,
        "realized_volatility": 0.1,
        "jump_frequency": 0.0,
        "median_spread_bps": 1.0,
        "median_hourly_volume": 1.0,
        "funding_rate_mean": 0.0,
    }
    payload[field] = "1"

    with pytest.raises(ValidationError) as error:
        SymbolProfileView(**payload)

    assert {item["loc"] for item in error.value.errors()} == {(field,)}


def test_data_gap_view_requires_positive_utc_interval() -> None:
    with pytest.raises(ValidationError, match="gap end must be after gap start"):
        DataGapView(
            gap_id=uuid4(),
            symbol="BTCUSDT",
            data_type=DataType.KLINE_1M,
            start=NOW,
            end=NOW,
            reason="archive_missing",
            status=DataGapStatus.OPEN,
            opened_at=NOW,
        )


@pytest.mark.parametrize("metric", [float("nan"), float("inf"), float("-inf")])
def test_symbol_profile_view_rejects_non_finite_metrics(metric: float) -> None:
    with pytest.raises(ValidationError) as error:
        SymbolProfileView(
            symbol="BTCUSDT",
            calculated_at=NOW,
            coverage_start=START,
            coverage_end=NOW,
            sample_count=1,
            coverage_fraction=1.0,
            realized_volatility=metric,
            jump_frequency=0.0,
            median_spread_bps=1.0,
            median_hourly_volume=1.0,
            funding_rate_mean=0.0,
        )

    assert "finite_number" in {item["type"] for item in error.value.errors()}


def test_eligibility_view_requires_reason_codes_when_ineligible() -> None:
    with pytest.raises(
        ValidationError, match="ineligible status requires at least one reason code"
    ):
        EligibilityView(
            symbol="BTCUSDT",
            eligible=False,
            reason_codes=(),
            evaluated_at=NOW,
        )


def test_eligibility_view_exposes_machine_readable_reason_codes() -> None:
    view = EligibilityView(
        symbol="BTCUSDT",
        eligible=False,
        reason_codes=(
            EligibilityReasonCode.METADATA_UNVERIFIED,
            EligibilityReasonCode.UNREPAIRED_GAP,
        ),
        evaluated_at=NOW,
    )

    assert view.reason_codes == (
        EligibilityReasonCode.METADATA_UNVERIFIED,
        EligibilityReasonCode.UNREPAIRED_GAP,
    )


def test_stream_state_view_requires_utc_timestamps() -> None:
    with pytest.raises(ValidationError, match="timestamps must be timezone-aware"):
        StreamStateView(
            symbol="BTCUSDT",
            stream_name="kline_1m",
            status=StreamStatus.CONNECTED,
            last_event_at=NOW.replace(tzinfo=None),
            updated_at=NOW,
        )


def test_market_data_health_view_exposes_the_source_mode() -> None:
    health = MarketDataHealthView(
        source_mode=SourceMode.PROXY,
        archive_healthy=True,
        rest_healthy=False,
        worker_heartbeat_at=NOW,
        streams=(
            StreamStateView(
                symbol="BTCUSDT",
                stream_name="kline_1m",
                status=StreamStatus.CONNECTED,
                last_event_at=NOW,
                updated_at=NOW,
            ),
        ),
        checked_at=NOW,
    )

    assert health.source_mode is SourceMode.PROXY
