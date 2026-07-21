import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_research.contracts.manifest import DataType
from crypto_research.market.profile import (
    ApprovedProfileData,
    CatalogProfileService,
    compute_symbol_profile,
)

START = datetime(2026, 7, 20, tzinfo=UTC)
END = START + timedelta(hours=1)


def fixture(
    symbol: str,
    closes: tuple[str, ...],
    spreads: tuple[tuple[str, str], ...],
    volumes: tuple[str, ...],
    funding: tuple[str, ...],
    *,
    approved: bool = True,
) -> ApprovedProfileData:
    minute_times = tuple(START + timedelta(minutes=index) for index in range(len(closes)))
    return ApprovedProfileData(
        symbol=symbol,
        coverage_start=START,
        coverage_end=END,
        approved=approved,
        closes=tuple(zip(minute_times, map(Decimal, closes), strict=True)),
        best_bid_ask=tuple(
            (minute_times[index], Decimal(bid), Decimal(ask))
            for index, (bid, ask) in enumerate(spreads)
        ),
        volumes=tuple(
            (minute_times[index], Decimal(volume))
            for index, volume in enumerate(volumes)
        ),
        funding_rates=tuple(
            (START + timedelta(minutes=index * 10), Decimal(value))
            for index, value in enumerate(funding)
        ),
    )


def test_btc_and_pepe_profiles_use_independent_approved_evidence() -> None:
    btc = compute_symbol_profile(
        fixture(
            "BTCUSDT",
            ("100", "101", "100", "102"),
            (("99.9", "100.1"), ("100.9", "101.1")),
            ("10", "20", "30", "40"),
            ("0.0001", "0.0002"),
        ),
        calculated_at=END + timedelta(minutes=1),
    )
    pepe = compute_symbol_profile(
        fixture(
            "1000PEPEUSDT",
            ("0.000010", "0.000012", "0.000009", "0.000015"),
            (("0.000009", "0.000011"), ("0.000010", "0.000014")),
            ("1000000", "4000000", "9000000", "16000000"),
            ("0.001", "-0.002", "0.003"),
        ),
        calculated_at=END + timedelta(minutes=1),
    )

    assert btc.symbol == "BTCUSDT"
    assert pepe.symbol == "1000PEPEUSDT"
    assert btc.realized_volatility.value != pepe.realized_volatility.value
    assert btc.jump_frequency.value != pepe.jump_frequency.value
    assert btc.median_spread_bps.value != pepe.median_spread_bps.value
    assert btc.median_hourly_volume.value != pepe.median_hourly_volume.value
    assert btc.funding_rate_mean.value != pepe.funding_rate_mean.value


def test_every_profile_metric_reports_its_own_samples_and_coverage() -> None:
    profile = compute_symbol_profile(
        fixture(
            "BTCUSDT",
            ("100", "101", "102", "103"),
            (("99", "101"), ("100", "102")),
            ("1", "2", "3", "4"),
            ("0.001", "0.002", "0.003"),
        ),
        calculated_at=END + timedelta(minutes=1),
    )

    assert profile.realized_volatility.sample_count == 3
    assert profile.jump_frequency.sample_count == 3
    assert profile.median_spread_bps.sample_count == 2
    assert profile.median_hourly_volume.sample_count == 1
    assert profile.funding_rate_mean.sample_count == 3
    assert 0 < profile.realized_volatility.coverage_fraction < 1
    assert 0 < profile.median_spread_bps.coverage_fraction < 1


def test_profile_rejects_non_catalog_approved_data() -> None:
    with pytest.raises(ValueError, match="approved"):
        compute_symbol_profile(
            fixture(
                "BTCUSDT",
                ("100", "101"),
                (("99", "101"),),
                ("1", "2"),
                ("0.001",),
                approved=False,
            ),
            calculated_at=END + timedelta(minutes=1),
        )


def test_profile_service_reads_each_metric_only_through_approved_catalog() -> None:
    class Catalog:
        def __init__(self) -> None:
            self.calls: list[DataType] = []

        async def read(self, _symbol, data_type, _start, _end):
            self.calls.append(data_type)
            if data_type is DataType.KLINE_1M:
                return (
                    {"open_time": int(START.timestamp() * 1000), "close": "100", "volume": "2"},
                    {
                        "open_time": int((START + timedelta(minutes=1)).timestamp() * 1000),
                        "close": "101",
                        "volume": "3",
                    },
                )
            if data_type is DataType.BEST_BID_ASK:
                return (
                    {
                        "event_time": int(START.timestamp() * 1000),
                        "best_bid": "99",
                        "best_ask": "101",
                    },
                )
            return (
                {
                    "funding_time": int(START.timestamp() * 1000),
                    "funding_rate": "0.001",
                },
            )

    async def scenario() -> None:
        catalog = Catalog()
        profile = await CatalogProfileService(catalog).compute(
            "BTCUSDT", START, END, calculated_at=END + timedelta(minutes=1)
        )

        assert profile.realized_volatility.sample_count == 1
        assert catalog.calls == [
            DataType.KLINE_1M,
            DataType.BEST_BID_ASK,
            DataType.FUNDING,
        ]

    asyncio.run(scenario())
