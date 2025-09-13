# src/btc_sentiment/pipelines/price_sentiment_pipeline.py
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from ..config.config import get_settings
from ..adapters.binance_price_adapter import BinancePriceAdapter
from ..utils.io import load_daily_sentiment, save_dataframe
from ..utils.visualizations import plot_sentiment_with_price


def _calc_window(days_back: int, include_today: bool = True) -> tuple[datetime, datetime]:
    end = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0)
    if include_today:
        start = (end - timedelta(days=days_back - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        end = (end - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        start = (end - timedelta(days=days_back - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start, end


def run_price_sentiment_pipeline(
    days_back: int = 60,
    include_today: bool = True,
    sentiment_path: Optional[str] = "data/processed/daily_sentiment.parquet",
    output_path: Optional[str] = "data/processed/daily_sentiment_with_price.parquet",
) -> pd.DataFrame:
    """
    Load daily sentiment, fetch Binance daily BTC prices, merge, save, and plot.
    """
    # 1) load sentiment (list -> DataFrame)
    records = load_daily_sentiment(sentiment_path)
    if not records:
        print("❌ No sentiment records found. Run your sentiment pipeline first.")
        return pd.DataFrame()

    df = pd.DataFrame([r.dict() for r in records]).copy()
    # normalize date to daily
    df["date"] = (
        pd.to_datetime(df["date"], utc=True, errors="coerce")
          .dt.tz_convert(None)
          .dt.normalize()
    )
    df = df.sort_values(["source", "date"]).reset_index(drop=True)

    # 2) fetch price window (align to the same range as df)
    settings = get_settings()
    start, end = _calc_window(days_back, include_today)
    # (Optionally, you can widen to df.min()/df.max() if you prefer)
    symbol = getattr(settings, "BINANCE_SYMBOL", "BTCUSDT")

    price_adapter = BinancePriceAdapter()
    px = price_adapter.fetch_daily_close(symbol=symbol, start=start, end=end)
    if px.empty:
        print("⚠️ No price data returned from Binance.")
        # still return your df untouched
        return df

    # 3) merge
    merged = df.merge(px[["date", "close"]].rename(columns={"close": "btc_close"}),
                      on="date", how="left")
    # optional derived columns
    merged["btc_ret_pct"] = merged["btc_close"].pct_change() * 100.0

    # sanity prints
    print(f"📅 Window: {merged['date'].min().date()} → {merged['date'].max().date()} "
          f"• days: {merged['date'].nunique()}")
    if merged["btc_close"].notna().any():
        corr = merged["avg_score"].corr(merged["btc_close"])
        ret_corr = merged["avg_score"].corr(merged["btc_ret_pct"])
        print(f"🔗 Corr(sentiment, price): {corr:.3f} • Corr(sentiment, daily %ret): {ret_corr:.3f}")

    # 4) save and plot
    save_dataframe(merged, output_path)
    plot_sentiment_with_price(merged)

    return merged
