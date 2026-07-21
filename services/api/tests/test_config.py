import pytest
from pydantic import ValidationError

from crypto_research.config import Settings


def test_defaults_are_chinese_utc_and_local_only() -> None:
    settings = Settings(_env_file=None)
    assert settings.default_locale == "zh-CN"
    assert settings.internal_timezone == "UTC"
    assert settings.allowed_hosts == ["127.0.0.1", "localhost"]


def test_production_rejects_example_session_secret() -> None:
    with pytest.raises(ValidationError, match="production session secret"):
        Settings(
            _env_file=None,
            environment="production",
            session_secret="change-me-at-least-32-characters",
        )


def test_market_data_defaults_are_bounded_and_routed() -> None:
    settings = Settings(_env_file=None)

    assert settings.archive_base_url == "https://data.binance.vision"
    assert settings.rest_base_url == "https://fapi.binance.com"
    assert settings.public_ws_base_url == "wss://fstream.binance.com/public"
    assert settings.market_ws_base_url == "wss://fstream.binance.com/market"
    assert settings.history_max_days == 366
    assert settings.live_stale_after_seconds == 120
    assert settings.market_worker_id == "market-worker"
    assert settings.proxy_mode == "auto"
    assert settings.http_proxy_url is None


@pytest.mark.parametrize("field", ["archive_base_url", "rest_base_url", "public_ws_base_url"])
def test_market_data_urls_require_an_absolute_network_url(field: str) -> None:
    with pytest.raises(ValidationError, match="absolute network URL"):
        Settings(_env_file=None, **{field: "/not-a-network-url"})


@pytest.mark.parametrize(
    "field",
    ["archive_base_url", "rest_base_url", "public_ws_base_url", "market_ws_base_url"],
)
def test_production_rejects_source_urls_with_credentials(field: str) -> None:
    with pytest.raises(ValidationError, match="credentials"):
        Settings(
            _env_file=None,
            environment="production",
            session_secret="x" * 32,
            **{field: "https://user:secret@example.test"},
        )


@pytest.mark.parametrize("proxy_url", ["http://proxy.example.test:17891", "http://10.0.0.2:17891"])
def test_production_rejects_non_loopback_proxy(proxy_url: str) -> None:
    with pytest.raises(ValidationError, match="loopback"):
        Settings(
            _env_file=None,
            environment="production",
            session_secret="x" * 32,
            http_proxy_url=proxy_url,
        )


@pytest.mark.parametrize(
    "proxy_url", ["http://localhost:17891", "http://127.0.0.1:17891", "http://[::1]:17891"]
)
def test_production_accepts_loopback_proxy(proxy_url: str) -> None:
    settings = Settings(
        _env_file=None,
        environment="production",
        session_secret="x" * 32,
        http_proxy_url=proxy_url,
    )

    assert settings.http_proxy_url == proxy_url


@pytest.mark.parametrize(
    "proxy_url",
    [
        "ftp://127.0.0.1:17891",
        "http://user:secret@127.0.0.1:17891",
        "http://127.0.0.1",
        "http://127.0.0.1:70000",
    ],
)
def test_production_rejects_unsafe_loopback_proxy_shapes(proxy_url: str) -> None:
    with pytest.raises(ValidationError, match="loopback HTTP proxy"):
        Settings(
            _env_file=None,
            environment="production",
            session_secret="x" * 32,
            http_proxy_url=proxy_url,
        )


def test_proxy_only_mode_requires_an_explicit_proxy_url() -> None:
    with pytest.raises(ValidationError, match="proxy URL"):
        Settings(
            _env_file=None,
            environment="production",
            session_secret="x" * 32,
            proxy_mode="proxy",
        )
