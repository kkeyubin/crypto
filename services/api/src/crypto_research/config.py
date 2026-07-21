from functools import lru_cache
from ipaddress import ip_address
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CRYPTO_",
        extra="ignore",
    )

    environment: Literal["development", "test", "production"] = "development"
    database_url: str = Field(
        default="postgresql+asyncpg://crypto:crypto@localhost:5432/crypto_research",
        repr=False,
    )
    data_root: Path = Path("./data")
    internal_timezone: Literal["UTC"] = "UTC"
    default_locale: Literal["zh-CN", "en"] = "zh-CN"
    allowed_hosts: list[str] = Field(default_factory=lambda: ["127.0.0.1", "localhost"])
    session_secret: str = "change-me-at-least-32-characters"
    archive_base_url: str = "https://data.binance.vision"
    rest_base_url: str = "https://fapi.binance.com"
    public_ws_base_url: str = "wss://fstream.binance.com/public"
    market_ws_base_url: str = "wss://fstream.binance.com/market"
    history_max_days: int = Field(default=366, ge=1, le=366)
    live_stale_after_seconds: int = Field(default=120, ge=1)
    market_worker_id: str = Field(default="market-worker", min_length=1, max_length=128)
    proxy_mode: Literal["auto", "direct", "proxy"] = "auto"
    http_proxy_url: str | None = Field(default=None, repr=False)

    @field_validator(
        "archive_base_url",
        "rest_base_url",
        "public_ws_base_url",
        "market_ws_base_url",
    )
    @classmethod
    def validate_market_data_url(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https", "ws", "wss"} or parsed.hostname is None:
            raise ValueError("market data URL must be an absolute network URL")
        return value

    @model_validator(mode="after")
    def reject_example_production_secret(self) -> "Settings":
        if self.environment == "production" and self.session_secret.startswith("change-me"):
            raise ValueError("production session secret must not use the example value")
        if len(self.session_secret) < 32:
            raise ValueError("session secret must contain at least 32 characters")
        if self.environment == "production":
            source_urls = (
                self.archive_base_url,
                self.rest_base_url,
                self.public_ws_base_url,
                self.market_ws_base_url,
            )
            if any(_has_userinfo(value) for value in source_urls):
                raise ValueError("production source URLs must not include credentials")
            if self.http_proxy_url is not None and not _is_loopback_proxy(self.http_proxy_url):
                raise ValueError("production proxy URL must use a loopback hostname or IP")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()


def _has_userinfo(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.username is not None or parsed.password is not None


def _is_loopback_proxy(value: str) -> bool:
    hostname = urlparse(value).hostname
    if hostname is None:
        return False
    if hostname.lower() == "localhost":
        return True
    try:
        return ip_address(hostname).is_loopback
    except ValueError:
        return False
