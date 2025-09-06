from functools import lru_cache
from typing import List, Tuple

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    X_BEARER_TOKEN: str = Field(..., env="X_BEARER_TOKEN")
    TG_API_ID: int = Field(..., env="TG_API_ID")
    TG_API_HASH: str = Field(..., env="TG_API_HASH")

    TELEGRAM_CHANNELS: List[str] = [
        "bitcoin_industry", "CryptoWorldNews", "cointelegraph"
    ]
    AGG_WINDOW_MINUTES: int = 60
    THRESHOLDS: Tuple[int, int] = (45, 55)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
