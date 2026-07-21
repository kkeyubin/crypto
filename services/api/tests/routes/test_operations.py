import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import InterfaceError, OperationalError

from crypto_research.api import create_app
from crypto_research.config import Settings

from .conftest import FakeMarketDataControl, ReadyProbe


def test_market_data_operations_are_redacted_and_fail_closed(client: TestClient) -> None:
    response = client.get("/api/operations/market-data")

    assert response.status_code == 200
    assert response.json()["source_mode"] == "proxy"
    assert response.json()["rest_healthy"] is False
    assert "proxy_url" not in response.text
    assert "source_url" not in response.text
    assert "password" not in response.text


def test_operations_rejects_unknown_query_without_echoing_it(client: TestClient) -> None:
    secret = "https://user:secret@localhost:17891/?token=secret"
    response = client.get(
        "/api/operations/market-data", params={"proxy_url": secret}
    )

    assert response.status_code == 422
    assert secret not in response.text
    assert "user:secret" not in response.text


def test_persistent_state_unavailability_is_503_but_programming_errors_escape() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])

    class Unavailable(FakeMarketDataControl):
        async def market_data_health(self):
            raise OperationalError("SELECT worker_heartbeats", {}, ConnectionError("offline"))

    with TestClient(
        create_app(settings, ReadyProbe(), market_data_service=Unavailable())
    ) as client:
        response = client.get("/api/operations/market-data")
    assert response.status_code == 503
    assert response.json() == {
        "detail": "persistent market-data state is unavailable"
    }
    assert "SELECT" not in response.text

    class Broken(FakeMarketDataControl):
        async def market_data_health(self):
            raise RuntimeError("programming error sentinel")

    with TestClient(
        create_app(settings, ReadyProbe(), market_data_service=Broken())
    ) as client, pytest.raises(RuntimeError, match="programming error sentinel"):
        client.get("/api/operations/market-data")

    class MisusedDriver(FakeMarketDataControl):
        async def market_data_health(self):
            raise InterfaceError(
                "driver called through the wrong interface",
                {},
                RuntimeError("programming misuse"),
            )

    with TestClient(
        create_app(settings, ReadyProbe(), market_data_service=MisusedDriver())
    ) as client, pytest.raises(InterfaceError):
        client.get("/api/operations/market-data")
