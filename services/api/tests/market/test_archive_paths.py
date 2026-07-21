from datetime import UTC, datetime, timedelta, timezone

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


def test_funding_planner_uses_only_complete_monthly_archives() -> None:
    objects = plan_archives(
        DatasetKind.FUNDING_RATE,
        "BTCUSDT",
        utc("2026-05-01T00:00:00"),
        utc("2026-07-01T00:00:00"),
        as_of=utc("2026-07-21T12:00:00"),
    )

    assert [item.source.cadence for item in objects] == [
        ArchiveCadence.MONTHLY,
        ArchiveCadence.MONTHLY,
    ]
    assert [item.url for item in objects] == [
        "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/"
        "BTCUSDT-fundingRate-2026-05.zip",
        "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/"
        "BTCUSDT-fundingRate-2026-06.zip",
    ]


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-06-14T00:00:00", "2026-06-15T00:00:00"),
        ("2026-05-02T00:00:00", "2026-07-01T00:00:00"),
        ("2026-05-01T00:00:00", "2026-06-30T00:00:00"),
    ],
)
def test_funding_planner_rejects_partial_months_before_creating_objects(
    start: str, end: str
) -> None:
    with pytest.raises(ValueError, match="complete UTC calendar months"):
        plan_archives(
            DatasetKind.FUNDING_RATE,
            "BTCUSDT",
            utc(start),
            utc(end),
            as_of=utc("2026-07-21T12:00:00"),
        )


def test_planner_rejects_non_midnight_or_non_utc_ranges() -> None:
    with pytest.raises(ValueError, match="midnight UTC"):
        plan_archives(
            DatasetKind.KLINES,
            "BTCUSDT",
            utc("2024-01-01T01:00:00"),
            utc("2024-01-02T00:00:00"),
        )


def test_planner_rejects_current_open_day_and_future_days() -> None:
    as_of = utc("2024-02-01T12:34:56")

    with pytest.raises(ValueError, match="closed UTC days"):
        plan_archives(
            DatasetKind.KLINES,
            "BTCUSDT",
            utc("2024-02-01T00:00:00"),
            utc("2024-02-02T00:00:00"),
            as_of=as_of,
        )
    with pytest.raises(ValueError, match="closed UTC days"):
        plan_archives(
            DatasetKind.KLINES,
            "BTCUSDT",
            utc("2024-02-02T00:00:00"),
            utc("2024-02-03T00:00:00"),
            as_of=as_of,
        )


def test_planner_allows_range_ending_at_current_utc_midnight() -> None:
    objects = plan_archives(
        DatasetKind.KLINES,
        "BTCUSDT",
        utc("2024-01-31T00:00:00"),
        utc("2024-02-01T00:00:00"),
        as_of=utc("2024-02-01T12:34:56"),
    )

    assert len(objects) == 1


@pytest.mark.parametrize(
    "as_of",
    [
        datetime(2024, 2, 1, 12, 34, 56),
        datetime(2024, 2, 1, 12, 34, 56, tzinfo=timezone(timedelta(hours=8))),
    ],
)
def test_planner_requires_utc_as_of(as_of: datetime) -> None:
    with pytest.raises(ValueError, match="as_of must be UTC"):
        plan_archives(
            DatasetKind.KLINES,
            "BTCUSDT",
            utc("2024-01-31T00:00:00"),
            utc("2024-02-01T00:00:00"),
            as_of=as_of,
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
