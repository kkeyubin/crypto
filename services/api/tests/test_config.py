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
