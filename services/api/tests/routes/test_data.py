from datetime import timedelta

from fastapi.testclient import TestClient

from .conftest import BTC_JOB, END, START


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
        "/api/symbols/BTCUSDT/backfills", json=backfill_payload("PEPEUSDT")
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
    pepe_partitions = client.get("/api/symbols/PEPEUSDT/partitions")
    btc_gaps = client.get("/api/symbols/BTCUSDT/gaps")
    pepe_gaps = client.get("/api/symbols/PEPEUSDT/gaps")
    btc_profile = client.get("/api/symbols/BTCUSDT/profile")
    pepe_profile = client.get("/api/symbols/PEPEUSDT/profile")
    btc_eligibility = client.get("/api/symbols/BTCUSDT/eligibility")
    pepe_eligibility = client.get("/api/symbols/PEPEUSDT/eligibility")
    streams = client.get("/api/symbols/BTCUSDT/streams")

    assert [item["symbol"] for item in btc_partitions.json()] == ["BTCUSDT"]
    assert pepe_partitions.json() == []
    assert btc_gaps.json() == []
    assert [item["symbol"] for item in pepe_gaps.json()] == ["PEPEUSDT"]
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
