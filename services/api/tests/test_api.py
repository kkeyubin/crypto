from fastapi.testclient import TestClient

from crypto_research.api import create_app
from crypto_research.config import Settings


class ReadyProbe:
    def __init__(self, is_ready: bool = True) -> None:
        self.is_ready = is_ready

    async def ready(self) -> bool:
        return self.is_ready

    async def close(self) -> None:
        return None


def test_live_health_has_stable_shape() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])
    with TestClient(create_app(settings, ReadyProbe())) as client:
        response = client.get("/api/health/live")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "api",
        "version": "0.1.0",
    }


def test_untrusted_host_is_rejected() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])
    with TestClient(create_app(settings, ReadyProbe())) as client:
        response = client.get("/api/health/live", headers={"host": "evil.example"})
    assert response.status_code == 400


def test_ready_health_uses_database_probe() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])
    with TestClient(create_app(settings, ReadyProbe())) as client:
        response = client.get("/api/health/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "service": "api"}


def test_ready_health_fails_closed() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])
    with TestClient(create_app(settings, ReadyProbe(False))) as client:
        response = client.get("/api/health/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "service": "api"}
