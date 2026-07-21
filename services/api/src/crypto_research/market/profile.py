from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Protocol

from crypto_research.contracts.manifest import DataType


@dataclass(frozen=True)
class ApprovedProfileData:
    symbol: str
    coverage_start: datetime
    coverage_end: datetime
    approved: bool
    closes: tuple[tuple[datetime, Decimal], ...]
    best_bid_ask: tuple[tuple[datetime, Decimal, Decimal], ...]
    volumes: tuple[tuple[datetime, Decimal], ...]
    funding_rates: tuple[tuple[datetime, Decimal], ...]

    def __post_init__(self) -> None:
        _coverage(self.coverage_start, self.coverage_end)


@dataclass(frozen=True)
class ProfileMetric:
    value: float
    sample_count: int
    coverage_fraction: float


@dataclass(frozen=True)
class SymbolProfile:
    symbol: str
    calculated_at: datetime
    coverage_start: datetime
    coverage_end: datetime
    realized_volatility: ProfileMetric
    jump_frequency: ProfileMetric
    median_spread_bps: ProfileMetric
    median_hourly_volume: ProfileMetric
    funding_rate_mean: ProfileMetric


class ApprovedCatalogReader(Protocol):
    async def read(
        self,
        symbol: str,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> tuple[Mapping[str, object], ...]: ...


class CatalogProfileService:
    """Build profiles exclusively from the approved catalog query boundary."""

    def __init__(self, catalog: ApprovedCatalogReader) -> None:
        self._catalog = catalog

    async def compute(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        calculated_at: datetime,
    ) -> SymbolProfile:
        _coverage(start, end)
        klines = await self._catalog.read(symbol, DataType.KLINE_1M, start, end)
        quotes = await self._catalog.read(symbol, DataType.BEST_BID_ASK, start, end)
        funding = await self._catalog.read(symbol, DataType.FUNDING, start, end)
        data = ApprovedProfileData(
            symbol=symbol,
            coverage_start=start,
            coverage_end=end,
            approved=True,
            closes=tuple(
                (_from_milliseconds(row["open_time"]), _decimal(row["close"], "close"))
                for row in klines
            ),
            best_bid_ask=tuple(
                (
                    _from_milliseconds(row["event_time"]),
                    _decimal(row["best_bid"], "best_bid"),
                    _decimal(row["best_ask"], "best_ask"),
                )
                for row in quotes
            ),
            volumes=tuple(
                (
                    _from_milliseconds(row["open_time"]),
                    _decimal(row["volume"], "volume"),
                )
                for row in klines
            ),
            funding_rates=tuple(
                (
                    _from_milliseconds(row["funding_time"]),
                    _decimal(row["funding_rate"], "funding_rate"),
                )
                for row in funding
            ),
        )
        return compute_symbol_profile(data, calculated_at=calculated_at)


def compute_symbol_profile(
    data: ApprovedProfileData, *, calculated_at: datetime
) -> SymbolProfile:
    if not data.approved:
        raise ValueError("symbol profiles may use only catalog-approved data")
    calculated_at = _utc(calculated_at, "calculated_at")
    if calculated_at < data.coverage_end:
        raise ValueError("profile calculation cannot precede its coverage")
    _validate_samples(data)
    expected_minutes = max(
        1, int((data.coverage_end - data.coverage_start).total_seconds() // 60)
    )

    closes = [float(value) for _, value in data.closes]
    if any(value <= 0 for value in closes):
        raise ValueError("close values must be positive")
    returns = [
        math.log(current / previous)
        for previous, current in zip(closes, closes[1:], strict=False)
    ]
    realized = math.sqrt(sum(value * value for value in returns)) if returns else 0.0
    jump_frequency = _jump_frequency(returns)
    return_coverage = _fraction(len(returns), max(1, expected_minutes - 1))

    spreads = [
        float((ask - bid) / ((ask + bid) / Decimal(2)) * Decimal(10_000))
        for _, bid, ask in data.best_bid_ask
    ]
    if any(value < 0 for value in spreads):
        raise ValueError("best ask cannot be below best bid")

    hourly: dict[datetime, Decimal] = defaultdict(Decimal)
    for observed_at, volume in data.volumes:
        hour = observed_at.replace(minute=0, second=0, microsecond=0)
        hourly[hour] += volume
    hourly_volumes = [float(value) for _, value in sorted(hourly.items())]
    expected_hours = max(
        1, math.ceil((data.coverage_end - data.coverage_start).total_seconds() / 3600)
    )

    funding = [float(value) for _, value in data.funding_rates]
    expected_funding = max(
        1, math.ceil((data.coverage_end - data.coverage_start).total_seconds() / 28_800)
    )

    return SymbolProfile(
        symbol=data.symbol.strip().upper(),
        calculated_at=calculated_at,
        coverage_start=data.coverage_start,
        coverage_end=data.coverage_end,
        realized_volatility=ProfileMetric(
            realized, len(returns), return_coverage
        ),
        jump_frequency=ProfileMetric(
            jump_frequency, len(returns), return_coverage
        ),
        median_spread_bps=ProfileMetric(
            _median(spreads), len(spreads), _fraction(len(spreads), expected_minutes)
        ),
        median_hourly_volume=ProfileMetric(
            _median(hourly_volumes),
            len(hourly_volumes),
            _fraction(len(hourly_volumes), expected_hours),
        ),
        funding_rate_mean=ProfileMetric(
            statistics.fmean(funding) if funding else 0.0,
            len(funding),
            _fraction(len(funding), expected_funding),
        ),
    )


def _jump_frequency(returns: list[float]) -> float:
    if not returns:
        return 0.0
    absolute = [abs(value) for value in returns]
    center = statistics.median(absolute)
    mad = statistics.median(abs(value - center) for value in absolute)
    threshold = center + 3 * mad
    return sum(value > threshold for value in absolute) / len(absolute)


def _validate_samples(data: ApprovedProfileData) -> None:
    groups = (
        tuple((observed_at, (value,)) for observed_at, value in data.closes),
        tuple((observed_at, (bid, ask)) for observed_at, bid, ask in data.best_bid_ask),
        tuple((observed_at, (value,)) for observed_at, value in data.volumes),
        tuple((observed_at, (value,)) for observed_at, value in data.funding_rates),
    )
    for samples in groups:
        timestamps = []
        for observed_at, values in samples:
            observed_at = _utc(observed_at, "profile sample timestamp")
            if not data.coverage_start <= observed_at < data.coverage_end:
                raise ValueError("profile sample lies outside half-open coverage")
            if any(not value.is_finite() for value in values):
                raise ValueError("profile decimal samples must be finite")
            timestamps.append(observed_at)
        if timestamps != sorted(timestamps):
            raise ValueError("profile samples must be time ordered")


def _median(values: list[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


def _fraction(observed: int, expected: int) -> float:
    return min(1.0, observed / expected)


def _coverage(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start = _utc(start, "coverage_start")
    end = _utc(end, "coverage_end")
    if end <= start:
        raise ValueError("profile coverage must be a non-empty half-open range")
    return start, end


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field_name} must be UTC")
    return value.astimezone(UTC)


def _from_milliseconds(value: object) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("catalog timestamps must be integer milliseconds")
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, float):
        raise ValueError(f"{field_name} cannot originate from binary float")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be a decimal value") from error
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed
