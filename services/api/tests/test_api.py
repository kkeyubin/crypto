import pytest
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


def test_openapi_exposes_only_public_market_data_controls() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])
    with TestClient(create_app(settings, ReadyProbe())) as client:
        schema = client.get("/openapi.json").json()

    expected = {
        "/api/health/live",
        "/api/health/ready",
        "/api/symbols",
        "/api/symbols/{symbol}",
        "/api/symbols/{symbol}/backfills",
        "/api/backfills/{job_id}",
        "/api/symbols/{symbol}/partitions",
        "/api/symbols/{symbol}/gaps",
        "/api/symbols/{symbol}/profile",
        "/api/symbols/{symbol}/eligibility",
        "/api/symbols/{symbol}/streams",
        "/api/operations/market-data",
    }
    assert set(schema["paths"]) == expected
    serialized = str(schema).lower()
    assert not any(
        forbidden in serialized
        for forbidden in ("trade order", "credential", "wallet", "notification", "ai assessment")
    )


def test_production_app_uses_request_scoped_database_transactions() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])

    class Session:
        def __init__(self) -> None:
            self.commits = 0
            self.rollbacks = 0
            self.closes = 0

        async def commit(self) -> None:
            self.commits += 1

        async def rollback(self) -> None:
            self.rollbacks += 1

        async def close(self) -> None:
            self.closes += 1

        async def execute(self, _statement):
            class Scalars:
                @staticmethod
                def all():
                    return []

            class Result:
                @staticmethod
                def scalars():
                    return Scalars()

            return Result()

    class Database:
        def __init__(self) -> None:
            self.sessions: list[Session] = []
            self.closed = False

        def session(self) -> Session:
            session = Session()
            self.sessions.append(session)
            return session

        async def close(self) -> None:
            self.closed = True

    database = Database()
    with TestClient(
        create_app(
            settings,
            ReadyProbe(),
            database=database,
        )
    ) as client:
        assert client.get("/api/symbols").status_code == 200

    assert database.closed is True
    assert len(database.sessions) == 1
    assert database.sessions[0].commits == 1
    assert database.sessions[0].rollbacks == 0
    assert database.sessions[0].closes == 1


def test_request_transaction_rolls_back_and_closes_on_programming_error() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])

    class Session:
        def __init__(self) -> None:
            self.rollbacks = 0
            self.closes = 0

        async def commit(self) -> None:
            raise AssertionError("commit must not run")

        async def rollback(self) -> None:
            self.rollbacks += 1

        async def close(self) -> None:
            self.closes += 1

    class Database:
        def __init__(self) -> None:
            self.created = Session()

        def session(self) -> Session:
            return self.created

        async def close(self) -> None:
            return None

    class BrokenControl:
        async def list_symbols(self, *, limit: int, offset: int):
            del limit, offset
            raise RuntimeError("sentinel")

    database = Database()
    with TestClient(
        create_app(
            settings,
            ReadyProbe(),
            database=database,
            market_data_service_factory=lambda _session, _settings: BrokenControl(),
        )
    ) as client, pytest.raises(RuntimeError, match="sentinel"):
        client.get("/api/symbols")

    assert database.created.rollbacks == 1
    assert database.created.closes == 1


def test_request_transaction_closes_when_production_factory_raises() -> None:
    settings = Settings(_env_file=None, allowed_hosts=["testserver"])

    class Session:
        def __init__(self) -> None:
            self.rollbacks = 0
            self.closes = 0

        async def commit(self) -> None:
            raise AssertionError("commit must not run")

        async def rollback(self) -> None:
            self.rollbacks += 1

        async def close(self) -> None:
            self.closes += 1

    class Database:
        def __init__(self) -> None:
            self.created = Session()

        def session(self) -> Session:
            return self.created

        async def close(self) -> None:
            return None

    database = Database()

    def broken_factory(_session, _settings):
        raise RuntimeError("factory sentinel")

    with TestClient(
        create_app(
            settings,
            ReadyProbe(),
            database=database,
            market_data_service_factory=broken_factory,
        )
    ) as client, pytest.raises(RuntimeError, match="factory sentinel"):
        client.get("/api/symbols")

    assert database.created.rollbacks == 1
    assert database.created.closes == 1
