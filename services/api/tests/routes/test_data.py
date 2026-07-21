import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from crypto_research.contracts.data import EligibilityReasonCode, MetadataStatus
from crypto_research.db.repositories import StreamState
from crypto_research.market.binance.streams import streams_for_symbols
from crypto_research.market.control import _metadata_status, _profile_view
from crypto_research.market.eligibility import EligibilityDecision
from crypto_research.market.profile import ProfileMetric, SymbolProfile

from .conftest import BTC_JOB, END, START
from .test_control import END as CONTROL_END
from .test_control import NOW as CONTROL_NOW
from .test_control import START as CONTROL_START
from .test_control import configured_repository, service

_REALIZED_METRIC = ProfileMetric(0.2, 100, 0.9)
_JUMP_METRIC = ProfileMetric(0.01, 80, 0.8)
_SPREAD_METRIC = ProfileMetric(1.2, 20, 0.5)
_VOLUME_METRIC = ProfileMetric(50_000, 10, 0.75)
_FUNDING_METRIC = ProfileMetric(-0.0001, 3, 0.25)


def profile(
    *,
    realized: ProfileMetric = _REALIZED_METRIC,
    jumps: ProfileMetric = _JUMP_METRIC,
    spread: ProfileMetric = _SPREAD_METRIC,
    volume: ProfileMetric = _VOLUME_METRIC,
    funding: ProfileMetric = _FUNDING_METRIC,
) -> SymbolProfile:
    return SymbolProfile(
        symbol="BTCUSDT",
        calculated_at=datetime(2025, 1, 1, tzinfo=UTC),
        coverage_start=datetime(2024, 12, 1, tzinfo=UTC),
        coverage_end=datetime(2025, 1, 1, tzinfo=UTC),
        realized_volatility=realized,
        jump_frequency=jumps,
        median_spread_bps=spread,
        median_hourly_volume=volume,
        funding_rate_mean=funding,
    )


def decision(eligible: bool) -> EligibilityDecision:
    return EligibilityDecision(
        symbol="BTCUSDT",
        eligible=eligible,
        reason_codes=(
            () if eligible else (EligibilityReasonCode.UNREPAIRED_GAP,)
        ),
        evaluated_at=datetime(2025, 1, 1, tzinfo=UTC),
    )


def test_profile_mapping_keeps_each_metric_evidence_independent() -> None:
    view = _profile_view(profile())

    assert view.realized_volatility.model_dump() == {
        "value": 0.2,
        "sample_count": 100,
        "coverage_fraction": 0.9,
    }
    assert view.median_spread_bps.model_dump() == {
        "value": 1.2,
        "sample_count": 20,
        "coverage_fraction": 0.5,
    }
    assert view.funding_rate_mean.value == -0.0001


def test_profile_mapping_marks_a_zero_sample_metric_as_explicitly_missing() -> None:
    view = _profile_view(profile(spread=ProfileMetric(0.0, 0, 0.0)))

    assert view.median_spread_bps.value is None
    assert view.median_spread_bps.sample_count == 0


@pytest.mark.parametrize(
    "metadata_verified,profile_value,eligible,expected",
    [
        (False, profile(), True, MetadataStatus.METADATA_UNVERIFIED),
        (
            True,
            profile(spread=ProfileMetric(0.0, 0, 0.0)),
            False,
            MetadataStatus.PROFILE_BUILDING,
        ),
        (True, profile(), True, MetadataStatus.ELIGIBLE),
        (True, profile(), False, MetadataStatus.INELIGIBLE),
    ],
)
def test_metadata_status_reaches_terminal_state_only_after_profile_is_complete(
    metadata_verified: bool,
    profile_value: SymbolProfile,
    eligible: bool,
    expected: MetadataStatus,
) -> None:
    assert _metadata_status(
        metadata_verified=metadata_verified,
        profile=profile_value,
        eligibility=decision(eligible),
    ) is expected


def test_symbol_view_derives_verified_terminal_metadata_status_from_eligibility() -> None:
    async def scenario() -> None:
        repository = configured_repository()
        repository.summaries["BTCUSDT"] = SimpleNamespace(
            approved_data_types=("kline_1m", "mark_price", "funding"),
            archive_intervals={
                data_type: ((CONTROL_START, CONTROL_END),)
                for data_type in ("kline_1m", "mark_price", "funding")
            },
            metadata_verified=True,
            open_gap_count=0,
            job_statuses=("succeeded",),
        )
        repository.streams["BTCUSDT"] = tuple(
            StreamState(
                "BTCUSDT",
                stream.name,
                CONTROL_NOW - timedelta(seconds=1),
                "connected",
                {"source_mode": "direct"},
                CONTROL_NOW,
            )
            for stream in streams_for_symbols(("BTCUSDT",))
        )
        control = service(repository)

        eligible = await control.get_symbol("BTCUSDT")
        repository.summaries["BTCUSDT"].open_gap_count = 1
        ineligible = await control.get_symbol("BTCUSDT")

        assert eligible.metadata_status is MetadataStatus.ELIGIBLE
        assert ineligible.metadata_status is MetadataStatus.INELIGIBLE

    asyncio.run(scenario())


def backfill_payload(symbol: str = "BTCUSDT") -> dict[str, object]:
    return {
        "symbol": symbol,
        "data_types": ["kline_1m", "funding"],
        "start": START.isoformat(),
        "end": END.isoformat(),
        "include_agg_trades": False,
    }


def test_backfill_creation_is_typed_sorted_and_path_symbol_mismatch_is_409(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/symbols/BTCUSDT/backfills", json=backfill_payload()
    )
    conflict = client.post(
        "/api/symbols/BTCUSDT/backfills", json=backfill_payload("1000PEPEUSDT")
    )

    assert response.status_code == 200
    assert [item["data_type"] for item in response.json()] == ["funding", "kline_1m"]
    assert conflict.status_code == 409


def test_backfill_rejects_aggregate_trades_without_explicit_opt_in(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/symbols/BTCUSDT/backfills",
        json={**backfill_payload(), "data_types": ["agg_trade"]},
    )

    assert response.status_code == 422
    assert "explicit opt-in" in response.text
    string_boolean = client.post(
        "/api/symbols/BTCUSDT/backfills",
        json={
            **backfill_payload(),
            "data_types": ["agg_trade"],
            "include_agg_trades": "true",
        },
    )
    assert string_boolean.status_code == 422


def test_backfill_range_is_utc_nonempty_closed_and_bounded(client: TestClient) -> None:
    non_utc = client.post(
        "/api/symbols/BTCUSDT/backfills",
        json={**backfill_payload(), "start": "2026-07-01T08:00:00+08:00"},
    )
    inverted = client.post(
        "/api/symbols/BTCUSDT/backfills",
        json={**backfill_payload(), "start": END.isoformat(), "end": START.isoformat()},
    )
    too_large = client.post(
        "/api/symbols/BTCUSDT/backfills",
        json={
            **backfill_payload(),
            "end": (START + timedelta(days=367)).isoformat(),
        },
    )

    assert non_utc.status_code == 422
    assert inverted.status_code == 422
    assert too_large.status_code == 422


def test_get_backfill_is_isolated_and_returns_404(client: TestClient) -> None:
    existing = client.get(f"/api/backfills/{BTC_JOB}")
    missing = client.get("/api/backfills/00000000-0000-0000-0000-000000000999")

    assert existing.status_code == 200
    assert existing.json()["symbol"] == "BTCUSDT"
    assert missing.status_code == 404


def test_partitions_gaps_profile_eligibility_and_streams_are_symbol_scoped(
    client: TestClient,
) -> None:
    btc_partitions = client.get("/api/symbols/BTCUSDT/partitions")
    pepe_partitions = client.get("/api/symbols/1000PEPEUSDT/partitions")
    btc_gaps = client.get("/api/symbols/BTCUSDT/gaps")
    pepe_gaps = client.get("/api/symbols/1000PEPEUSDT/gaps")
    btc_profile = client.get("/api/symbols/BTCUSDT/profile")
    pepe_profile = client.get("/api/symbols/1000PEPEUSDT/profile")
    btc_eligibility = client.get("/api/symbols/BTCUSDT/eligibility")
    pepe_eligibility = client.get("/api/symbols/1000PEPEUSDT/eligibility")
    streams = client.get("/api/symbols/BTCUSDT/streams")

    assert [item["symbol"] for item in btc_partitions.json()] == ["BTCUSDT"]
    assert pepe_partitions.json() == []
    assert btc_gaps.json() == []
    assert [item["symbol"] for item in pepe_gaps.json()] == ["1000PEPEUSDT"]
    assert btc_profile.json()["realized_volatility"] != pepe_profile.json()[
        "realized_volatility"
    ]
    assert btc_eligibility.json()["reason_codes"] != pepe_eligibility.json()[
        "reason_codes"
    ]
    assert [item["symbol"] for item in streams.json()] == ["BTCUSDT"]


def test_data_lists_use_bounded_pagination_and_reject_arbitrary_queries(
    client: TestClient,
) -> None:
    assert (
        client.get("/api/symbols/BTCUSDT/partitions", params={"limit": 101}).status_code
        == 422
    )
    response = client.get(
        "/api/symbols/BTCUSDT/gaps",
        params={"source_url": "https://user:secret@example.invalid/?token=secret"},
    )
    assert response.status_code == 422
    assert "user:secret" not in response.text


def test_create_backfill_rejects_unknown_query_without_echoing_it(
    client: TestClient,
) -> None:
    secret = "https://user:secret@example.invalid/?token=secret"

    response = client.post(
        "/api/symbols/BTCUSDT/backfills",
        params={"source_url": secret},
        json=backfill_payload(),
    )

    assert response.status_code == 422
    assert secret not in response.text
    assert "user:secret" not in response.text
