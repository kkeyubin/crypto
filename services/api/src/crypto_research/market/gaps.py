from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from crypto_research.contracts.manifest import DataType


class GapReason(StrEnum):
    MISSING_MINUTE = "missing_minute_open_time"
    AGGREGATE_TRADE_ID = "aggregate_trade_id_discontinuity"
    SOURCE_UNKNOWN = "source_unknown"
    PARTITION_COVERAGE = "partition_coverage"
    ARCHIVE_MISSING = "archive_missing"


class GapStatus(StrEnum):
    OPEN = "open"
    REPAIRED = "repaired"


@dataclass(frozen=True, order=True)
class TimeRange:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        _utc_range(self.start, self.end)


@dataclass(frozen=True)
class ApprovedCoverage:
    symbol: str
    data_type: DataType
    time_range: TimeRange
    partition_id: str
    approved: bool = True
    recovered_id_start: int | None = None
    recovered_id_end: int | None = None

    def __post_init__(self) -> None:
        if not self.partition_id:
            raise ValueError("approved coverage requires a partition ID")
        if (self.recovered_id_start is None) != (self.recovered_id_end is None):
            raise ValueError("recovered ID evidence requires both range boundaries")
        if (
            self.recovered_id_start is not None
            and self.recovered_id_end is not None
            and self.recovered_id_end < self.recovered_id_start
        ):
            raise ValueError("recovered ID range end cannot precede start")


@dataclass(frozen=True)
class RepairAttempt:
    attempted_at: datetime
    source: str
    result: str


@dataclass(frozen=True)
class DetectedGap:
    gap_id: str
    symbol: str
    data_type: DataType
    start: datetime
    end: datetime
    reason: GapReason
    details: Mapping[str, object] = field(default_factory=dict)
    status: GapStatus = GapStatus.OPEN
    repair_history: tuple[RepairAttempt, ...] = ()
    repaired_at: datetime | None = None

    def __post_init__(self) -> None:
        _utc_range(self.start, self.end)


def detect_minute_gaps(
    symbol: str,
    open_times: Iterable[datetime],
    coverage: TimeRange,
    *,
    data_type: DataType = DataType.KLINE_1M,
) -> tuple[DetectedGap, ...]:
    if data_type not in {DataType.KLINE_1M, DataType.MARK_PRICE}:
        raise ValueError("minute gap detection requires a minute-bar data type")
    minute = timedelta(minutes=1)
    boundaries = (coverage.start, coverage.end)
    if any(value.second or value.microsecond for value in boundaries):
        raise ValueError("minute coverage boundaries must align to UTC minutes")
    observed = {_utc(value, "open_time") for value in open_times}
    if any(value < coverage.start or value >= coverage.end for value in observed):
        raise ValueError("open_time is outside requested half-open coverage")
    expected: list[datetime] = []
    cursor = coverage.start
    while cursor < coverage.end:
        expected.append(cursor)
        cursor += minute
    missing = [value for value in expected if value not in observed]
    ranges = _coalesce_points(missing, minute)
    return tuple(
        _gap(symbol, data_type, item, GapReason.MISSING_MINUTE)
        for item in ranges
    )


def detect_aggregate_trade_id_gaps(
    symbol: str, events: Iterable[tuple[int, datetime]]
) -> tuple[DetectedGap, ...]:
    ordered = list(events)
    if ordered != sorted(ordered, key=lambda item: item[0]):
        raise ValueError("aggregate trades must be ordered by ID")
    gaps: list[DetectedGap] = []
    for previous, current in zip(ordered, ordered[1:], strict=False):
        previous_id, previous_at = previous
        current_id, current_at = current
        previous_at = _utc(previous_at, "event timestamp")
        current_at = _utc(current_at, "event timestamp")
        if current_id <= previous_id:
            raise ValueError("aggregate trade IDs must be unique and increasing")
        if current_id == previous_id + 1:
            continue
        end = max(current_at, previous_at + timedelta(milliseconds=1))
        gaps.append(
            _gap(
                symbol,
                DataType.AGG_TRADE,
                TimeRange(previous_at, end),
                GapReason.AGGREGATE_TRADE_ID,
                {
                    "missing_id_start": previous_id + 1,
                    "missing_id_end": current_id - 1,
                },
            )
        )
    return tuple(gaps)


def source_unknown_gap(
    symbol: str,
    data_type: DataType,
    start: datetime,
    end: datetime,
) -> DetectedGap:
    return _gap(symbol, data_type, TimeRange(start, end), GapReason.SOURCE_UNKNOWN)


def detect_partition_coverage_gaps(
    symbol: str,
    data_type: DataType,
    requested: TimeRange,
    approved: Iterable[TimeRange],
) -> tuple[DetectedGap, ...]:
    uncovered = _subtract_coverage(requested, approved)
    return tuple(
        _gap(symbol, data_type, item, GapReason.PARTITION_COVERAGE)
        for item in uncovered
    )


class GapLedger:
    def __init__(self) -> None:
        self._gaps: dict[str, DetectedGap] = {}

    def record(self, gap: DetectedGap) -> DetectedGap:
        existing = self._gaps.get(gap.gap_id)
        if existing is not None:
            return existing
        self._gaps[gap.gap_id] = gap
        return gap

    def reconcile(
        self,
        gap_id: str,
        approved: Iterable[ApprovedCoverage],
        attempted_at: datetime,
        source: str,
    ) -> DetectedGap:
        attempted_at = _utc(attempted_at, "attempted_at")
        try:
            gap = self._gaps[gap_id]
        except KeyError as error:
            raise ValueError(f"gap does not exist: {gap_id}") from error
        approved = tuple(approved)
        fully_covered = approved_evidence_repairs(gap, approved)
        attempt = RepairAttempt(
            attempted_at=attempted_at,
            source=source,
            result="repaired" if fully_covered else "partial",
        )
        updated = replace(
            gap,
            status=GapStatus.REPAIRED if fully_covered else GapStatus.OPEN,
            repair_history=(*gap.repair_history, attempt),
            repaired_at=attempted_at if fully_covered else None,
        )
        self._gaps[gap_id] = updated
        return updated


def approved_coverage_covers(
    symbol: str,
    data_type: DataType,
    requested: TimeRange,
    approved: Iterable[ApprovedCoverage],
) -> bool:
    matching = (
        item.time_range
        for item in approved
        if item.approved
        and item.symbol.strip().upper() == symbol.strip().upper()
        and item.data_type is data_type
    )
    return not _subtract_coverage(requested, matching)


def approved_evidence_repairs(
    gap: DetectedGap, approved: Iterable[ApprovedCoverage]
) -> bool:
    approved = tuple(approved)
    if not approved_coverage_covers(
        gap.symbol,
        gap.data_type,
        TimeRange(gap.start, gap.end),
        approved,
    ):
        return False
    if gap.reason is not GapReason.AGGREGATE_TRADE_ID:
        return True
    required_start = gap.details.get("missing_id_start")
    required_end = gap.details.get("missing_id_end")
    if not isinstance(required_start, int) or not isinstance(required_end, int):
        return False
    intervals = sorted(
        (
            (item.recovered_id_start, item.recovered_id_end)
            for item in approved
            if item.approved
            and item.symbol.strip().upper() == gap.symbol.strip().upper()
            and item.data_type is gap.data_type
            and item.recovered_id_start is not None
            and item.recovered_id_end is not None
        ),
        key=lambda item: item[0],
    )
    cursor = required_start
    for start, end in intervals:
        assert start is not None and end is not None
        if start > cursor:
            break
        cursor = max(cursor, end + 1)
        if cursor > required_end:
            return True
    return False


def _subtract_coverage(
    requested: TimeRange, approved: Iterable[TimeRange]
) -> tuple[TimeRange, ...]:
    clipped = sorted(
        (
            TimeRange(max(item.start, requested.start), min(item.end, requested.end))
            for item in approved
            if item.start < requested.end and item.end > requested.start
        ),
        key=lambda item: (item.start, item.end),
    )
    gaps: list[TimeRange] = []
    cursor = requested.start
    for item in clipped:
        if item.start > cursor:
            gaps.append(TimeRange(cursor, item.start))
        cursor = max(cursor, item.end)
    if cursor < requested.end:
        gaps.append(TimeRange(cursor, requested.end))
    return tuple(gaps)


def _coalesce_points(
    points: list[datetime], width: timedelta
) -> tuple[TimeRange, ...]:
    if not points:
        return ()
    ranges: list[TimeRange] = []
    start = previous = points[0]
    for point in points[1:]:
        if point != previous + width:
            ranges.append(TimeRange(start, previous + width))
            start = point
        previous = point
    ranges.append(TimeRange(start, previous + width))
    return tuple(ranges)


def _gap(
    symbol: str,
    data_type: DataType,
    time_range: TimeRange,
    reason: GapReason,
    details: Mapping[str, object] | None = None,
) -> DetectedGap:
    symbol = symbol.strip().upper()
    identity = "|".join(
        (
            symbol,
            data_type.value,
            time_range.start.isoformat(),
            time_range.end.isoformat(),
            reason.value,
        )
    )
    return DetectedGap(
        gap_id=str(uuid5(NAMESPACE_URL, identity)),
        symbol=symbol,
        data_type=data_type,
        start=time_range.start,
        end=time_range.end,
        reason=reason,
        details=details or {},
    )


def _utc_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start = _utc(start, "start")
    end = _utc(end, "end")
    if end <= start:
        raise ValueError("coverage must be a non-empty half-open UTC range")
    return start, end


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field_name} must be UTC")
    return value.astimezone(UTC)
