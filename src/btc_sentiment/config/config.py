# src/btc_sentiment/config.py
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Union

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_env_file() -> str:
    here = Path(__file__).resolve()
    for p in (here.parents[2] / ".env", here.parents[1] / ".env", Path(".env")):
        if p.exists():
            return str(p)
    return ".env"


def _parse_list_str(s: Optional[str]) -> List[Union[str, int]]:
    """
    Accept list-like env values in flexible formats:
      - JSON array:  '["a", -100123]'
      - CSV string:  'a, -100123, @foo'
      - With inline comments: '["a"]  # none'
    Returns [] for empty/None.
    """
    if not s:
        return []
    s = s.strip()
    # strip inline comments
    if "#" in s:
        s = s.split("#", 1)[0].strip()
    if not s or s.upper() == "NONE":
        return []
    # try JSON array
    if s.startswith("[") and s.endswith("]"):
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return val
        except Exception:
            pass
    # CSV fallback
    tokens = [t.strip().strip("'").strip('"') for t in s.split(",")]
    out: List[Union[str, int]] = []
    for t in tokens:
        if not t:
            continue
        if re.fullmatch(r"-?\d+", t):
            try:
                out.append(int(t))
                continue
            except Exception:
                pass
        out.append(t)
    return out


class Settings(BaseSettings):
    # --- API keys / tokens ---
    X_BEARER_TOKEN: Optional[str] = Field(default=None, env="X_BEARER_TOKEN")
    TG_API_ID: int = Field(..., env="TG_API_ID")
    TG_API_HASH: str = Field(..., env="TG_API_HASH")

    # --- Raw strings from env (we parse them ourselves) ---
    TELEGRAM_CHANNELS_STR: Optional[str] = Field(default=None, alias="TELEGRAM_CHANNELS")
    TELEGRAM_GROUPS_STR: Optional[str] = Field(default=None, alias="TELEGRAM_GROUPS")

    # --- Market data ---
    BINANCE_SYMBOL: str = Field(default="BTCUSDT", env="BINANCE_SYMBOL")

    model_config = SettingsConfigDict(
        env_file=_resolve_env_file(),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Parsed, read-only properties
    @property
    def TELEGRAM_CHANNELS(self) -> List[Union[str, int]]:
        return _parse_list_str(self.TELEGRAM_CHANNELS_STR)

    @property
    def TELEGRAM_GROUPS(self) -> List[Union[str, int]]:
        return _parse_list_str(self.TELEGRAM_GROUPS_STR)

    @property
    def BINANCE_SYMBOL_NORMALIZED(self) -> str:
        return (self.BINANCE_SYMBOL or "BTCUSDT").strip().upper()


@lru_cache()
def get_settings() -> Settings:
    return Settings()
