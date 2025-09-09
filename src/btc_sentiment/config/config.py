from functools import lru_cache
from typing import List, Tuple, Union

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    X_BEARER_TOKEN: str = Field(..., env="X_BEARER_TOKEN")
    TG_API_ID: int = Field(..., env="TG_API_ID")
    TG_API_HASH: str = Field(..., env="TG_API_HASH")

    TELEGRAM_CHANNELS: List[str] = [
        # "bitcoin",
        # "CryptoWorldNews"
    ]
    
    TELEGRAM_GROUPS: List[Union[str, int]] = [
        # -1001305631383  # Use integer format
        # "-1001305631383"  # String format didn't work
        "Insider_leak_of_theday"
    ]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
