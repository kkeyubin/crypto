from fastapi.testclient import TestClient

from .conftest import END, START, FakeMarketDataControl


def payload(symbol: str = "ETHUSDT") -> dict[str, object]:
    return {
        "symbol": symbol,
        "history_start": START.isoformat(),
        "history_end": END.isoformat(),
        "include_agg_trades": False,
    }


def test_symbol_list_is_stably_sorted_and_bounded(client: TestClient) -> None:
    response = client.get("/api/symbols", params={"limit": 1, "offset": 1})

    assert response.status_code == 200
    assert [item["symbol"] for item in response.json()] == ["PEPEUSDT"]
    assert client.get("/api/symbols", params={"limit": 101}).status_code == 422


def test_add_symbol_is_idempotent_and_conflicting_identity_is_409(
    client: TestClient, control: FakeMarketDataControl
) -> None:
    first = client.post("/api/symbols", json=payload())
    second = client.post("/api/symbols", json=payload())
    conflict = client.post(
        "/api/symbols",
        json={**payload(), "history_end": (END.replace(hour=1)).isoformat()},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert control.audit_actions == ["symbol_added"]
    assert conflict.status_code == 409


def test_get_and_disable_symbol_never_delete_history(
    client: TestClient, control: FakeMarketDataControl
) -> None:
    before = client.get("/api/symbols/BTCUSDT")
    first = client.delete("/api/symbols/BTCUSDT")
    second = client.delete("/api/symbols/BTCUSDT")
    after = client.get("/api/symbols/BTCUSDT")

    assert before.status_code == 200
    assert first.status_code == 200
    assert first.json()["enabled"] is False
    assert second.json() == first.json()
    assert after.json() == first.json()
    assert "BTCUSDT" in control.symbols
    assert control.audit_actions == ["symbol_disabled"]


def test_unknown_symbol_and_untrusted_symbol_syntax_fail_closed(client: TestClient) -> None:
    assert client.get("/api/symbols/SOLUSDT").status_code == 404
    assert client.get("/api/symbols/btcusdt").status_code == 422
    assert client.delete("/api/symbols/BTC-USDT").status_code == 422


def test_symbol_mutation_rejects_non_utc_range_and_aggregate_extras(
    client: TestClient,
) -> None:
    non_utc = client.post(
        "/api/symbols",
        json={**payload(), "history_start": "2026-07-01T08:00:00+08:00"},
    )
    inverted = client.post(
        "/api/symbols",
        json={
            **payload(),
            "history_start": END.isoformat(),
            "history_end": START.isoformat(),
        },
    )
    secret = "https://user:secret@example.invalid/archive.zip?token=secret"
    arbitrary_source = client.post(
        "/api/symbols", json={**payload(), "source_url": secret}
    )
    string_boolean = client.post(
        "/api/symbols", json={**payload(), "include_agg_trades": "true"}
    )

    assert non_utc.status_code == 422
    assert inverted.status_code == 422
    assert arbitrary_source.status_code == 422
    assert string_boolean.status_code == 422
    assert secret not in arbitrary_source.text
