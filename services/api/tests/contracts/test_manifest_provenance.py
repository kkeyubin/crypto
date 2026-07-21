import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from crypto_research.contracts.manifest import (
    ArchiveCadence,
    ArchiveDataset,
    BinanceArchiveSource,
    BinanceRestEndpoint,
    BinanceRestSource,
    BinanceStream,
    BinanceWebSocketSource,
    DataManifest,
    DataType,
    DeduplicationMethod,
    ValidationState,
)
from crypto_research.contracts.strategy import InstrumentRef

NOW = datetime(2025, 1, 2, tzinfo=UTC)
MONTH_START = datetime(2025, 1, 1, tzinfo=UTC)
INSTRUMENT = InstrumentRef(venue="BINANCE", market="USD_M_PERPETUAL", symbol="BTCUSDT")


def manifest_payload(**overrides: object) -> dict[str, object]:
    return {
        "manifest_id": uuid4(),
        "instrument": INSTRUMENT,
        "data_type": DataType.KLINE_1M,
        "start": MONTH_START,
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
            period_start=MONTH_START,
        ),
        "raw_path": "raw/binance/usdm/BTCUSDT/kline_1m/date=2025-01-02/source.zip",
        "normalized_path": "normalized/binance/usdm/BTCUSDT/kline_1m/date=2025-01-02/data.parquet",
        "source_checksum": "b" * 64,
        "normalized_checksum": "c" * 64,
        "row_count": 1,
        "validation_state": ValidationState.VALIDATED,
        "primary_key_fields": ("open_time",),
        "deduplication_method": DeduplicationMethod.REJECT_DUPLICATES,
        "duplicates_removed": 0,
        **overrides,
    }


def test_manifest_parses_discriminated_structured_source() -> None:
    payload = manifest_payload()
    payload["instrument"] = INSTRUMENT.model_dump(mode="json")
    payload["source"] = {
        "kind": "binance_rest",
        "endpoint": "klines",
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 1500,
    }

    manifest = DataManifest.model_validate_json(json.dumps(payload, default=str))

    assert isinstance(manifest.source, BinanceRestSource)
    assert manifest.source.resolved_url == (
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=1500"
    )


@pytest.mark.parametrize(
    ("cadence", "dataset", "interval", "period_start", "expected_url"),
    [
        (
            ArchiveCadence.DAILY,
            ArchiveDataset.KLINES,
            "1m",
            NOW,
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-01-02.zip",
        ),
        (
            ArchiveCadence.MONTHLY,
            ArchiveDataset.KLINES,
            "1m",
            MONTH_START,
            "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-01.zip",
        ),
        (
            ArchiveCadence.DAILY,
            ArchiveDataset.MARK_PRICE_KLINES,
            "1m",
            NOW,
            "https://data.binance.vision/data/futures/um/daily/markPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2025-01-02.zip",
        ),
        (
            ArchiveCadence.MONTHLY,
            ArchiveDataset.MARK_PRICE_KLINES,
            "1m",
            MONTH_START,
            "https://data.binance.vision/data/futures/um/monthly/markPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2025-01.zip",
        ),
        (
            ArchiveCadence.DAILY,
            ArchiveDataset.FUNDING_RATE,
            None,
            NOW,
            "https://data.binance.vision/data/futures/um/daily/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2025-01-02.zip",
        ),
        (
            ArchiveCadence.MONTHLY,
            ArchiveDataset.FUNDING_RATE,
            None,
            MONTH_START,
            "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2025-01.zip",
        ),
        (
            ArchiveCadence.DAILY,
            ArchiveDataset.AGG_TRADES,
            None,
            NOW,
            "https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-01-02.zip",
        ),
        (
            ArchiveCadence.MONTHLY,
            ArchiveDataset.AGG_TRADES,
            None,
            MONTH_START,
            "https://data.binance.vision/data/futures/um/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-01.zip",
        ),
    ],
)
def test_archive_source_computes_official_urls(
    cadence: ArchiveCadence,
    dataset: ArchiveDataset,
    interval: str | None,
    period_start: datetime,
    expected_url: str,
) -> None:
    source = BinanceArchiveSource(
        kind="binance_archive",
        cadence=cadence,
        dataset=dataset,
        symbol="BTCUSDT",
        interval=interval,
        period_start=period_start,
    )

    assert source.resolved_url == expected_url


@pytest.mark.parametrize(
    "source",
    [
        {
            "kind": "binance_archive",
            "cadence": "monthly",
            "dataset": "klines",
            "symbol": "BTCUSDT",
            "interval": "1m",
            "period_start": NOW,
        },
        {
            "kind": "binance_archive",
            "cadence": "daily",
            "dataset": "funding_rate",
            "symbol": "BTCUSDT",
            "interval": "1m",
            "period_start": NOW,
        },
        {
            "kind": "binance_archive",
            "cadence": "daily",
            "dataset": "agg_trades",
            "symbol": "BTCUSDT",
            "period_start": NOW.replace(hour=1),
        },
    ],
)
def test_archive_source_rejects_invalid_period_and_interval(source: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        BinanceArchiveSource.model_validate(source)


@pytest.mark.parametrize(
    ("endpoint", "interval", "start_time", "end_time", "from_id", "limit", "expected_url"),
    [
        (
            BinanceRestEndpoint.KLINES,
            "1m",
            1,
            2,
            None,
            1500,
            "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&startTime=1&endTime=2&limit=1500",
        ),
        (
            BinanceRestEndpoint.MARK_PRICE_KLINES,
            "1m",
            None,
            None,
            None,
            1,
            "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&limit=1",
        ),
        (
            BinanceRestEndpoint.FUNDING_RATE,
            None,
            1,
            2,
            None,
            1000,
            "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=1&endTime=2&limit=1000",
        ),
        (
            BinanceRestEndpoint.AGG_TRADES,
            None,
            None,
            None,
            3,
            1000,
            "https://fapi.binance.com/fapi/v1/aggTrades?symbol=BTCUSDT&fromId=3&limit=1000",
        ),
        (
            BinanceRestEndpoint.EXCHANGE_INFO,
            None,
            None,
            None,
            None,
            1,
            "https://fapi.binance.com/fapi/v1/exchangeInfo?symbol=BTCUSDT",
        ),
    ],
)
def test_rest_source_computes_canonical_query_order(
    endpoint: BinanceRestEndpoint,
    interval: str | None,
    start_time: int | None,
    end_time: int | None,
    from_id: int | None,
    limit: int,
    expected_url: str,
) -> None:
    source = BinanceRestSource(
        kind="binance_rest",
        endpoint=endpoint,
        symbol="BTCUSDT",
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        from_id=from_id,
        limit=limit,
    )

    assert source.resolved_url == expected_url


@pytest.mark.parametrize(
    "source",
    [
        {"endpoint": BinanceRestEndpoint.KLINES, "interval": "1m", "limit": 1501},
        {"endpoint": BinanceRestEndpoint.FUNDING_RATE, "interval": "1m", "limit": 1},
        {"endpoint": BinanceRestEndpoint.AGG_TRADES, "from_id": 1, "start_time": 1, "limit": 1},
        {
            "endpoint": BinanceRestEndpoint.AGG_TRADES,
            "start_time": 0,
            "end_time": 3_600_001,
            "limit": 1,
        },
        {"endpoint": BinanceRestEndpoint.FUNDING_RATE, "start_time": 2, "end_time": 1, "limit": 1},
        {"endpoint": BinanceRestEndpoint.FUNDING_RATE, "start_time": "1", "limit": 1},
    ],
)
def test_rest_source_rejects_invalid_limits_and_time_rules(source: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        BinanceRestSource(kind="binance_rest", symbol="BTCUSDT", **source)


@pytest.mark.parametrize(
    ("stream", "expected_url"),
    [
        (BinanceStream.KLINE_1M, "wss://fstream.binance.com/market/ws/btcusdt@kline_1m"),
        (BinanceStream.MARK_PRICE, "wss://fstream.binance.com/market/ws/btcusdt@markPrice@1s"),
        (BinanceStream.AGG_TRADE, "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"),
        (BinanceStream.BEST_BID_ASK, "wss://fstream.binance.com/public/ws/btcusdt@bookTicker"),
    ],
)
def test_websocket_source_computes_canonical_routed_urls(
    stream: BinanceStream, expected_url: str
) -> None:
    source = BinanceWebSocketSource(kind="binance_websocket", stream=stream, symbol="BTCUSDT")

    assert source.resolved_url == expected_url


@pytest.mark.parametrize(
    ("source", "data_type", "instrument"),
    [
        (
            BinanceArchiveSource(
                kind="binance_archive",
                cadence=ArchiveCadence.DAILY,
                dataset=ArchiveDataset.FUNDING_RATE,
                symbol="BTCUSDT",
                period_start=NOW,
            ),
            DataType.KLINE_1M,
            INSTRUMENT,
        ),
        (
            BinanceWebSocketSource(
                kind="binance_websocket", stream=BinanceStream.BEST_BID_ASK, symbol="PEPEUSDT"
            ),
            DataType.BEST_BID_ASK,
            INSTRUMENT,
        ),
    ],
)
def test_manifest_rejects_source_data_type_or_symbol_mismatches(
    source: object, data_type: DataType, instrument: InstrumentRef
) -> None:
    with pytest.raises(ValidationError):
        DataManifest(**manifest_payload(source=source, data_type=data_type, instrument=instrument))


def test_resolved_urls_are_read_only_serialization_output() -> None:
    manifest = DataManifest(**manifest_payload())
    serialized = manifest.model_dump(mode="json")

    assert serialized["source"]["resolved_url"].endswith("BTCUSDT-1m-2025-01-01.zip")
    with pytest.raises(ValidationError, match="resolved_url"):
        DataManifest.model_validate_json(json.dumps(serialized))

    del serialized["source"]["resolved_url"]
    assert DataManifest.model_validate_json(json.dumps(serialized)) == manifest


def test_provenance_models_reject_caller_supplied_urls() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        BinanceWebSocketSource(
            kind="binance_websocket",
            stream=BinanceStream.KLINE_1M,
            symbol="BTCUSDT",
            resolved_url="wss://attacker.invalid/",
        )
    with pytest.raises(ValidationError, match="Extra inputs"):
        DataManifest(**manifest_payload(source_object_url="https://attacker.invalid/"))


def test_manifest_binds_archive_coverage_to_daily_source_period() -> None:
    with pytest.raises(ValidationError, match="archive manifest coverage"):
        DataManifest(
            **manifest_payload(
                start=MONTH_START,
                end=NOW,
                source=BinanceArchiveSource(
                    kind="binance_archive",
                    cadence=ArchiveCadence.DAILY,
                    dataset=ArchiveDataset.KLINES,
                    symbol="BTCUSDT",
                    interval="1m",
                    period_start=NOW,
                ),
            )
        )


def test_manifest_binds_rest_time_bounds_to_half_open_milliseconds() -> None:
    start_ms = int(MONTH_START.timestamp() * 1000)
    end_ms = int(NOW.timestamp() * 1000)
    rest = BinanceRestSource(
        kind="binance_rest",
        endpoint=BinanceRestEndpoint.KLINES,
        symbol="BTCUSDT",
        interval="1m",
        start_time=start_ms,
        end_time=end_ms - 1,
        limit=1500,
    )
    assert DataManifest(**manifest_payload(source=rest)).source is rest

    with pytest.raises(ValidationError, match="REST manifest coverage"):
        DataManifest(
            **manifest_payload(
                source=rest.model_copy(update={"start_time": start_ms + 1})
            )
        )


def test_manifest_rejects_sub_millisecond_coverage_boundaries() -> None:
    with pytest.raises(ValidationError, match="millisecond-aligned"):
        DataManifest(
            **manifest_payload(start=MONTH_START.replace(microsecond=1))
        )
