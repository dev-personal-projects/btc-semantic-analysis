# src/btc_sentiment/adapters/binance_price_adapter.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, List
import time
import requests
import pandas as pd


class BinancePriceAdapter:
    """Lightweight REST client for Binance spot daily klines (public, no API key)."""

    BASE_URL = "https://api.binance.com/api/v3/klines"

    @staticmethod
    def _to_ms(dt: datetime) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def fetch_daily_close(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Fetch daily OHLC and return a DataFrame with columns:
        ['date', 'open', 'high', 'low', 'close', 'volume'].
        'date' is normalized to midnight (UTC, naive).
        """
        start_ms = self._to_ms(start)
        end_ms = self._to_ms(end)

        all_rows: List[list] = []
        cursor = start_ms

        while True:
            params = {
                "symbol": symbol,
                "interval": "1d",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": min(limit, 1000),
            }
            resp = requests.get(self.BASE_URL, params=params, timeout=15)
            if resp.status_code == 429:
                time.sleep(1.2)
                continue
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break

            all_rows.extend(batch)

            # next page: start at last open time + 1 ms
            last_open = batch[-1][0]
            next_cursor = last_open + 1
            if next_cursor > end_ms:
                break
            if next_cursor == cursor:
                break
            cursor = next_cursor

        if not all_rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(all_rows)
        # Binance kline columns:
        # 0 open time, 1 open, 2 high, 3 low, 4 close, 5 volume, 6 close time, ...
        df = df[[0, 1, 2, 3, 4, 5]].copy()
        df.columns = ["open_time", "open", "high", "low", "close", "volume"]
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = (
            pd.to_datetime(df["open_time"], unit="ms", utc=True)
              .dt.tz_convert(None)
              .dt.normalize()
        )
        df = df.drop(columns=["open_time"]).sort_values("date")
        # filter to [start, end] inclusive
        start_norm = pd.to_datetime(start, utc=True).tz_convert(None).normalize()
        end_norm = pd.to_datetime(end, utc=True).tz_convert(None).normalize()
        df = df[(df["date"] >= start_norm) & (df["date"] <= end_norm)].reset_index(drop=True)
        return df[["date", "open", "high", "low", "close", "volume"]]
