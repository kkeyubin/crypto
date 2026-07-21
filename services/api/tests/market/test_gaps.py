from datetime import UTC, datetime, timedelta

from crypto_research.contracts.manifest import DataType
from crypto_research.market.gaps import (
    ApprovedCoverage,
    GapLedger,
    GapReason,
    GapStatus,
    TimeRange,
    detect_aggregate_trade_id_gaps,
    detect_minute_gaps,
    detect_partition_coverage_gaps,
    source_unknown_gap,
)

START = datetime(2026, 7, 20, tzinfo=UTC)


def test_detects_and_coalesces_missing_minute_open_times_with_half_open_coverage() -> None:
    gaps = detect_minute_gaps(
        "BTCUSDT",
        [START, START + timedelta(minutes=3)],
        TimeRange(START, START + timedelta(minutes=4)),
    )

    assert [(gap.start, gap.end, gap.reason) for gap in gaps] == [
        (
            START + timedelta(minutes=1),
            START + timedelta(minutes=3),
            GapReason.MISSING_MINUTE,
        )
    ]


def test_detects_aggregate_trade_id_discontinuity() -> None:
    gaps = detect_aggregate_trade_id_gaps(
        "PEPEUSDT",
        [(10, START), (11, START + timedelta(milliseconds=1)), (15, START + timedelta(seconds=1))],
    )

    assert len(gaps) == 1
    assert gaps[0].reason is GapReason.AGGREGATE_TRADE_ID
    assert gaps[0].details == {"missing_id_start": 12, "missing_id_end": 14}


def test_disconnect_window_is_an_explicit_source_unknown_gap() -> None:
    gap = source_unknown_gap(
        "BTCUSDT",
        DataType.BEST_BID_ASK,
        START,
        START + timedelta(seconds=12),
    )

    assert gap.reason is GapReason.SOURCE_UNKNOWN
    assert gap.start == START
    assert gap.end == START + timedelta(seconds=12)


def test_partition_coverage_uses_exact_half_open_utc_ranges() -> None:
    requested = TimeRange(START, START + timedelta(hours=4))
    approved = (
        TimeRange(START, START + timedelta(hours=1)),
        TimeRange(START + timedelta(hours=2), START + timedelta(hours=3)),
    )

    gaps = detect_partition_coverage_gaps("BTCUSDT", DataType.KLINE_1M, requested, approved)

    assert [(gap.start, gap.end) for gap in gaps] == [
        (START + timedelta(hours=1), START + timedelta(hours=2)),
        (START + timedelta(hours=3), START + timedelta(hours=4)),
    ]


def test_repair_history_is_retained_and_gap_closes_only_after_approved_coverage() -> None:
    ledger = GapLedger()
    gap = detect_partition_coverage_gaps(
        "BTCUSDT",
        DataType.KLINE_1M,
        TimeRange(START, START + timedelta(hours=2)),
        (),
    )[0]
    opened = ledger.record(gap)

    partial = ledger.reconcile(
        opened.gap_id,
        (
            ApprovedCoverage(
                "BTCUSDT",
                DataType.KLINE_1M,
                TimeRange(START, START + timedelta(hours=1)),
                "partition-1",
            ),
        ),
        START + timedelta(hours=3),
        "archive",
    )
    repaired = ledger.reconcile(
        opened.gap_id,
        (
            ApprovedCoverage(
                "BTCUSDT",
                DataType.KLINE_1M,
                TimeRange(START, START + timedelta(hours=1)),
                "partition-1",
            ),
            ApprovedCoverage(
                "BTCUSDT",
                DataType.KLINE_1M,
                TimeRange(START + timedelta(hours=1), START + timedelta(hours=2)),
                "partition-2",
            ),
        ),
        START + timedelta(hours=4),
        "archive",
    )

    assert partial.status is GapStatus.OPEN
    assert repaired.status is GapStatus.REPAIRED
    assert [attempt.result for attempt in repaired.repair_history] == ["partial", "repaired"]


def test_wrong_symbol_dataset_or_unapproved_ranges_cannot_close_gap() -> None:
    ledger = GapLedger()
    gap = ledger.record(
        detect_partition_coverage_gaps(
            "BTCUSDT",
            DataType.KLINE_1M,
            TimeRange(START, START + timedelta(hours=1)),
            (),
        )[0]
    )
    ranges = (
        ApprovedCoverage(
            "PEPEUSDT",
            DataType.KLINE_1M,
            TimeRange(gap.start, gap.end),
            "wrong-symbol",
        ),
        ApprovedCoverage(
            "BTCUSDT",
            DataType.FUNDING,
            TimeRange(gap.start, gap.end),
            "wrong-dataset",
        ),
        ApprovedCoverage(
            "BTCUSDT",
            DataType.KLINE_1M,
            TimeRange(gap.start, gap.end),
            "not-approved",
            approved=False,
        ),
    )

    result = ledger.reconcile(
        gap.gap_id, ranges, START + timedelta(hours=2), "archive"
    )
    assert result.status is GapStatus.OPEN


def test_aggregate_trade_gap_requires_approved_missing_id_evidence() -> None:
    ledger = GapLedger()
    gap = ledger.record(
        detect_aggregate_trade_id_gaps(
            "BTCUSDT",
            [(10, START), (15, START + timedelta(seconds=1))],
        )[0]
    )
    time_only = ApprovedCoverage(
        "BTCUSDT",
        DataType.AGG_TRADE,
        TimeRange(gap.start, gap.end),
        "time-only",
    )
    still_open = ledger.reconcile(
        gap.gap_id, (time_only,), START + timedelta(hours=1), "archive"
    )
    repaired = ledger.reconcile(
        gap.gap_id,
        (
            ApprovedCoverage(
                "BTCUSDT",
                DataType.AGG_TRADE,
                TimeRange(gap.start, gap.end),
                "id-evidence",
                recovered_id_start=11,
                recovered_id_end=14,
            ),
        ),
        START + timedelta(hours=2),
        "rest",
    )

    assert still_open.status is GapStatus.OPEN
    assert repaired.status is GapStatus.REPAIRED
