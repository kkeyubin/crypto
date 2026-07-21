import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
HEALTHCHECK = REPOSITORY_ROOT / "deploy" / "market-worker-healthcheck.py"


def load_healthcheck_module() -> object:
    specification = importlib.util.spec_from_file_location("market_worker_healthcheck", HEALTHCHECK)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_worker_healthcheck_accepts_only_the_host_network_database_endpoint() -> None:
    module = load_healthcheck_module()

    assert module.database_endpoint("127.0.0.1", "55432") == ("127.0.0.1", 55432)
    with pytest.raises(ValueError, match="worker database endpoint"):
        module.database_endpoint("postgres", "5432")
    with pytest.raises(ValueError, match="worker database endpoint"):
        module.database_endpoint("127.0.0.1", "5432")


def test_worker_healthcheck_requires_recent_running_or_degraded_heartbeat() -> None:
    module = load_healthcheck_module()
    now = datetime(2026, 7, 22, 0, 0, tzinfo=UTC)

    assert module.heartbeat_is_healthy("running", now - timedelta(seconds=20), now)
    assert module.heartbeat_is_healthy("degraded", now - timedelta(seconds=60), now)
    assert not module.heartbeat_is_healthy("stopped", now - timedelta(seconds=1), now)
    assert not module.heartbeat_is_healthy("running", now - timedelta(seconds=121), now)
    assert not module.heartbeat_is_healthy("running", None, now)
