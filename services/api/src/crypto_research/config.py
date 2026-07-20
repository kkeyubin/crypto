from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CRYPTO_",
        extra="ignore",
    )

    environment: Literal["development", "test", "production"] = "development"
    database_url: str = "postgresql+asyncpg://crypto:crypto@localhost:5432/crypto_research"
    data_root: Path = Path("./data")
    internal_timezone: Literal["UTC"] = "UTC"
    default_locale: Literal["zh-CN", "en"] = "zh-CN"
    allowed_hosts: list[str] = Field(default_factory=lambda: ["127.0.0.1", "localhost"])
    session_secret: str = "change-me-at-least-32-characters"

    @model_validator(mode="after")
    def reject_example_production_secret(self) -> "Settings":
        if self.environment == "production" and self.session_secret.startswith("change-me"):
            raise ValueError("production session secret must not use the example value")
        if len(self.session_secret) < 32:
            raise ValueError("session secret must contain at least 32 characters")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
