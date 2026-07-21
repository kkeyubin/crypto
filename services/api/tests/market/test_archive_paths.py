from datetime import UTC, datetime

import pytest

from crypto_research.contracts.manifest import ArchiveCadence, ArchiveDataset
from crypto_research.market.binance.archive_paths import ArchiveObject, DatasetKind, plan_archives


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def test_planner_prefers_whole_month_then_daily_edges() -> None:
    objects = plan_archives(
        DatasetKind.KLINES,
        "btcusdt",
        utc("2024-01-30T00:00:00"),
        utc("2024-03-03T00:00:00"),
    )

    assert [(item.source.cadence, item.source.period_start.date()) for item in objects] == [
        (ArchiveCadence.MONTHLY, utc("2024-02-01T00:00:00").date()),
        (ArchiveCadence.DAILY, utc("2024-01-30T00:00:00").date()),
        (ArchiveCadence.DAILY, utc("2024-01-31T00:00:00").date()),
        (ArchiveCadence.DAILY, utc("2024-03-01T00:00:00").date()),
        (ArchiveCadence.DAILY, utc("2024-03-02T00:00:00").date()),
    ]
    assert objects[0].symbol == "BTCUSDT"
    assert objects[0].url == (
        "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/"
        "BTCUSDT-1m-2024-02.zip"
    )
    assert objects[0].checksum_url == f"{objects[0].url}.CHECKSUM"


@pytest.mark.parametrize(
    ("dataset", "source_dataset", "url"),
    [
        (
            DatasetKind.KLINES,
            ArchiveDataset.KLINES,
            "daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-02-29.zip",
        ),
        (
            DatasetKind.MARK_PRICE_KLINES,
            ArchiveDataset.MARK_PRICE_KLINES,
            "daily/markPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2024-02-29.zip",
        ),
        (
            DatasetKind.FUNDING_RATE,
            ArchiveDataset.FUNDING_RATE,
            "daily/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-02-29.zip",
        ),
        (
            DatasetKind.AGG_TRADES,
            ArchiveDataset.AGG_TRADES,
            "daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-02-29.zip",
        ),
    ],
)
def test_planner_uses_exact_official_dataset_urls(
    dataset: DatasetKind, source_dataset: ArchiveDataset, url: str
) -> None:
    (item,) = plan_archives(
        dataset, "BTCUSDT", utc("2024-02-29T00:00:00"), utc("2024-03-01T00:00:00")
    )

    assert item.source.dataset is source_dataset
    assert item.url == f"https://data.binance.vision/data/futures/um/{url}"


def test_planner_rejects_non_midnight_or_non_utc_ranges() -> None:
    with pytest.raises(ValueError, match="midnight UTC"):
        plan_archives(
            DatasetKind.KLINES,
            "BTCUSDT",
            utc("2024-01-01T01:00:00"),
            utc("2024-01-02T00:00:00"),
        )


def test_archive_object_cannot_override_the_structured_official_source_url() -> None:
    original = plan_archives(
        DatasetKind.KLINES,
        "BTCUSDT",
        utc("2024-01-01T00:00:00"),
        utc("2024-01-02T00:00:00"),
    )[0]

    with pytest.raises(ValueError, match="resolved_url"):
        ArchiveObject(
            dataset=original.dataset,
            symbol=original.symbol,
            start=original.start,
            end=original.end,
            source=original.source,
            url="https://untrusted.example/archive.zip",
            checksum_url="https://untrusted.example/archive.zip.CHECKSUM",
        )

    with pytest.raises(ValueError, match="after start"):
        plan_archives(
            DatasetKind.KLINES,
            "BTCUSDT",
            utc("2024-01-02T00:00:00"),
            utc("2024-01-02T00:00:00"),
        )
