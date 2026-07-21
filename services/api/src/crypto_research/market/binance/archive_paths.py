from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from crypto_research.contracts.manifest import (
    ArchiveCadence,
    ArchiveDataset,
    BinanceArchiveSource,
)
from crypto_research.market.validation import normalize_symbol, require_utc_range


class DatasetKind(StrEnum):
    KLINES = "klines"
    MARK_PRICE_KLINES = "mark_price_klines"
    FUNDING_RATE = "funding_rate"
    AGG_TRADES = "agg_trades"


_ARCHIVE_DATASETS = {
    DatasetKind.KLINES: ArchiveDataset.KLINES,
    DatasetKind.MARK_PRICE_KLINES: ArchiveDataset.MARK_PRICE_KLINES,
    DatasetKind.FUNDING_RATE: ArchiveDataset.FUNDING_RATE,
    DatasetKind.AGG_TRADES: ArchiveDataset.AGG_TRADES,
}
_INTERVAL_DATASETS = {DatasetKind.KLINES, DatasetKind.MARK_PRICE_KLINES}


@dataclass(frozen=True)
class ArchiveObject:
    dataset: DatasetKind
    symbol: str
    start: datetime
    end: datetime
    source: BinanceArchiveSource
    url: str
    checksum_url: str

    def __post_init__(self) -> None:
        if self.url != self.source.resolved_url:
            raise ValueError("archive URL must equal the structured source resolved_url")
        if self.checksum_url != f"{self.source.resolved_url}.CHECKSUM":
            raise ValueError("archive checksum URL must be the structured source sibling CHECKSUM")
        if self.symbol != self.source.symbol or self.dataset.value != self.source.dataset.value:
            raise ValueError("archive object must match its structured source")
        expected_end = (
            _next_month(self.source.period_start)
            if self.source.cadence is ArchiveCadence.MONTHLY
            else self.source.period_start + timedelta(days=1)
        )
        if self.start != self.source.period_start or self.end != expected_end:
            raise ValueError("archive object range must match its structured source period")


def plan_archives(
    dataset: DatasetKind,
    symbol: str,
    start: datetime,
    end: datetime,
    *,
    as_of: datetime | None = None,
) -> tuple[ArchiveObject, ...]:
    """Build official archive objects, choosing full months before daily edge days.

    The range is half-open and consists of complete UTC calendar days. Missing
    archive handling deliberately belongs to the orchestration layer.
    """
    start, end = require_utc_range(start, end)
    if end > _current_utc_midnight(as_of):
        raise ValueError("archive ranges must contain only closed UTC days")
    normalized_symbol = normalize_symbol(symbol)
    month_starts = _complete_month_starts(start, end)
    monthly_days = {
        day
        for month in month_starts
        for day in _days(month, _next_month(month))
    }
    daily_days = [day for day in _days(start, end) if day not in monthly_days]
    objects = [
        _archive_object(dataset, normalized_symbol, month, ArchiveCadence.MONTHLY)
        for month in month_starts
    ]
    objects.extend(
        _archive_object(dataset, normalized_symbol, day, ArchiveCadence.DAILY) for day in daily_days
    )
    return tuple(objects)


def _current_utc_midnight(as_of: datetime | None) -> datetime:
    current = datetime.now(UTC) if as_of is None else as_of
    if current.tzinfo is not UTC:
        raise ValueError("as_of must be UTC")
    return current.replace(hour=0, minute=0, second=0, microsecond=0)


def _archive_object(
    dataset: DatasetKind, symbol: str, period_start: datetime, cadence: ArchiveCadence
) -> ArchiveObject:
    source = BinanceArchiveSource(
        kind="binance_archive",
        cadence=cadence,
        dataset=_ARCHIVE_DATASETS[dataset],
        symbol=symbol,
        interval="1m" if dataset in _INTERVAL_DATASETS else None,
        period_start=period_start,
    )
    end = (
        _next_month(period_start)
        if cadence is ArchiveCadence.MONTHLY
        else period_start + timedelta(days=1)
    )
    return ArchiveObject(
        dataset=dataset,
        symbol=symbol,
        start=period_start,
        end=end,
        source=source,
        url=source.resolved_url,
        checksum_url=f"{source.resolved_url}.CHECKSUM",
    )


def _complete_month_starts(start: datetime, end: datetime) -> tuple[datetime, ...]:
    first = start if start.day == 1 else _next_month(start.replace(day=1))
    months: list[datetime] = []
    current = first
    while _next_month(current) <= end:
        months.append(current)
        current = _next_month(current)
    return tuple(months)


def _next_month(value: datetime) -> datetime:
    if value.month == 12:
        return datetime(value.year + 1, 1, 1, tzinfo=UTC)
    return datetime(value.year, value.month + 1, 1, tzinfo=UTC)


def _days(start: datetime, end: datetime):
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)
