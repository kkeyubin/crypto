import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from crypto_research.contracts.manifest import DataType, MissingInterval
from crypto_research.market.backfill import (
    ArchiveBackfillStages,
    BackfillObject,
    _missing_intervals_from_evidence,
    _require_expected_source_checksum,
)
from crypto_research.market.binance.archive import ArchiveChecksumError
from crypto_research.market.binance.archive_paths import DatasetKind, plan_archives
from crypto_research.market.binance.normalization import KLINE_HEADERS, normalize_csv
from crypto_research.market.catalog import InMemoryCatalogRepository
from crypto_research.market.gaps import TimeRange
from crypto_research.market.storage import write_versioned_normalized_parquet

DAY_START = datetime(2024, 1, 1, tzinfo=UTC)
DAY_END = DAY_START + timedelta(days=1)


def test_replacement_download_rejects_checksum_change_after_recheck() -> None:
    work = BackfillObject(
        object_id="replacement-object",
        job_id="replacement-job",
        source_url="https://data.binance.vision/replacement.zip",
        start=DAY_START,
        end=DAY_END,
        source_checksum="b" * 64,
    )

    _require_expected_source_checksum(work, "b" * 64)
    with pytest.raises(ArchiveChecksumError, match="changed after replacement planning"):
        _require_expected_source_checksum(work, "c" * 64)


def test_content_gap_is_serialized_and_published_as_missing_interval(
    tmp_path: Path,
) -> None:
    missing_start = DAY_START + timedelta(hours=1)

    validation, manifest, gaps = asyncio.run(
        _validate_and_publish(tmp_path, missing_start=missing_start)
    )

    assert validation["missing_intervals"] == [
        {
            "start": missing_start.isoformat(),
            "end": (missing_start + timedelta(minutes=1)).isoformat(),
            "reason": "missing_minute_open_time",
        }
    ]
    assert manifest.missing_intervals == (
        MissingInterval(
            start=missing_start,
            end=missing_start + timedelta(minutes=1),
        ),
    )
    assert len(gaps) == 1


def test_complete_content_publishes_empty_missing_intervals(tmp_path: Path) -> None:
    validation, manifest, gaps = asyncio.run(
        _validate_and_publish(tmp_path, missing_start=None)
    )

    assert validation["missing_intervals"] == []
    assert manifest.missing_intervals == ()
    assert gaps == []


@pytest.mark.parametrize(
    ("serialized", "message"),
    [
        (
            [
                {
                    "start": DAY_START.isoformat(),
                    "end": (DAY_START + timedelta(minutes=1)).isoformat(),
                    "reason": [],
                }
            ],
            "unsupported reason",
        ),
        (
            [
                {
                    "start": DAY_START.isoformat(),
                    "end": (DAY_START + timedelta(minutes=2)).isoformat(),
                    "reason": "missing_minute_open_time",
                },
                {
                    "start": (DAY_START + timedelta(minutes=1)).isoformat(),
                    "end": (DAY_START + timedelta(minutes=3)).isoformat(),
                    "reason": "missing_minute_open_time",
                },
            ],
            "cannot overlap",
        ),
        (
            [
                {
                    "start": (DAY_START - timedelta(minutes=1)).isoformat(),
                    "end": DAY_START.isoformat(),
                    "reason": "missing_minute_open_time",
                }
            ],
            "outside archive coverage",
        ),
    ],
)
def test_missing_interval_evidence_rejects_malformed_overlap_or_out_of_range(
    serialized: list[dict[str, object]],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _missing_intervals_from_evidence(
            {"missing_intervals": serialized},
            TimeRange(DAY_START, DAY_END),
        )


async def _validate_and_publish(
    tmp_path: Path,
    *,
    missing_start: datetime | None,
):
    archive = plan_archives(
        DatasetKind.KLINES,
        "BTCUSDT",
        DAY_START,
        DAY_END,
        as_of=DAY_END + timedelta(days=1),
    )[0]
    data_root = tmp_path / "data"
    data_root.mkdir(mode=0o700)
    normalized = normalize_csv(
        DatasetKind.KLINES,
        _daily_kline_csv(missing_start),
        DAY_START,
        DAY_END,
    )
    source_checksum = "a" * 64
    stored = write_versioned_normalized_parquet(
        normalized,
        data_root,
        archive.symbol,
        archive.dataset,
        archive.start,
        source_checksum,
    )
    work = BackfillObject(
        object_id=f"object-{missing_start or 'complete'}",
        job_id="job-manifest-gap",
        source_url=archive.url,
        start=archive.start,
        end=archive.end,
        source_checksum=source_checksum,
        raw_path="raw/source.zip",
        normalized_path=stored.path.relative_to(data_root).as_posix(),
        normalized_checksum=stored.sha256,
        row_count=stored.row_count,
    )
    catalog = InMemoryCatalogRepository()
    gaps = []

    class GapSink:
        async def record_gap(self, gap) -> None:
            gaps.append(gap)

    stages = ArchiveBackfillStages(
        {archive.url: archive},
        data_root,
        tmp_path / "staging",
        object(),  # type: ignore[arg-type]
        catalog,
        GapSink(),
        clock=lambda: DAY_END + timedelta(days=1),
    )

    validation = await stages.validate(work)
    await stages.publish(work)
    approved = await catalog.approved(
        archive.symbol,
        DataType.KLINE_1M,
        archive.start,
        archive.end,
    )
    return validation, approved[0].manifest, gaps


def _daily_kline_csv(missing_start: datetime | None) -> bytes:
    rows = [",".join(KLINE_HEADERS)]
    cursor = DAY_START
    while cursor < DAY_END:
        if cursor != missing_start:
            open_time = int(cursor.timestamp() * 1000)
            rows.append(
                f"{open_time},1,1,1,1,1,{open_time + 59_999},1,1,1,1,0"
            )
        cursor += timedelta(minutes=1)
    return ("\n".join(rows) + "\n").encode()
