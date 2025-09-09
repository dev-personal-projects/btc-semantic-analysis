# src/btc_sentiment/pipelines/ingest_pipeline.py

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List

from ..config.config import get_settings
from ..adapters.x_api_adapter import XApiAdapter
from ..adapters.telegram_adapter import TelegramAdapter
from ..services.sentiment_service import SentimentService
from ..services.aggregator import Aggregator, DailySentiment
from ..utils.io import save_records
from ..utils.visualizations import plot_daily_sentiment


# -------- helpers --------

def _run_in_current_loop(coros):
    """Run coroutines inside the CURRENT event loop (Jupyter + nest_asyncio friendly)."""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(asyncio.gather(*coros))


def _calc_window(days_back: int, include_today: bool = True) -> tuple[datetime, datetime]:
    """
    Returns (start_date, end_date) as timezone-aware UTC datetimes aligned to day bounds.
    If include_today=True, the window ends today 23:59:59 and spans exactly `days_back` days.
    """
    end = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0)
    if include_today:
        start = (end - timedelta(days=days_back - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        end = (end - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        start = (end - timedelta(days=days_back - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start, end


# -------- pipelines --------

def run_ingest_pipeline(
    days_back: int = 1,
    tweets_per_day: int = 50,
    messages_limit: int = 200,
    output_path: str = "data/processed/daily_sentiment.parquet",
    include_today: bool = True,
) -> List[DailySentiment]:
    """
    End-to-end pipeline: X + Telegram → sentiment → daily aggregate (with full day coverage).
    """
    settings = get_settings()
    start_date, end_date = _calc_window(days_back, include_today)

    # --- X / Twitter ---
    x_adapter = XApiAdapter()
    tweets = x_adapter.fetch_recent_tweets(
        query="(bitcoin OR BTC) lang:en -is:retweet",
        limit=max(1, tweets_per_day * max(1, days_back)),
        start_time=start_date,
        end_time=end_date,
    )

    # --- Telegram (concurrent per channel/group) ---
    tg_adapter = TelegramAdapter()
    coros = []
    for ch in settings.TELEGRAM_CHANNELS or []:
        coros.append(
            tg_adapter.fetch_messages_for_channel(
                channel=ch, limit=messages_limit, since=start_date, until=end_date
            )
        )
    for g in settings.TELEGRAM_GROUPS or []:
        coros.append(
            tg_adapter.fetch_messages_for_channel(
                channel=g, limit=messages_limit, since=start_date, until=end_date
            )
        )

    messages = []
    if coros:
        for chunk in _run_in_current_loop(coros):
            messages.extend(chunk)

    # --- Sentiment annotation ---
    svc = SentimentService(low_thresh=45, high_thresh=55)
    tweet_records = [{"date": t.created_at, "source": "twitter", "text": t.text} for t in tweets]
    msg_records = [{"date": m.date, "source": "telegram", "text": m.text} for m in messages]

    for rec_list in (tweet_records, msg_records):
        if not rec_list:
            continue
        anns = svc.annotate([r["text"] for r in rec_list])
        for r, a in zip(rec_list, anns):
            r["norm_score"] = a.norm_score
            r["label"] = a.label

    all_records = tweet_records + msg_records
    if not all_records:
        print("❌ No data fetched (check credentials and group/channel access).")
        return []

    # --- Aggregate with full day coverage in requested window ---
    sources = []
    if msg_records:
        sources.append("telegram")
    if tweet_records:
        sources.append("twitter")
    if not sources:
        sources = ["telegram"]  # default if empty

    daily = Aggregator.aggregate(
        all_records,
        start_date=start_date,
        end_date=end_date,
        fill_missing=True,
        neutral_fill=50.0,
        sources=sources,
    )

    expected_days = (end_date.date() - start_date.date()).days + 1
    print(f"📅 Requested window: {start_date.date()} → {end_date.date()} "
          f"({expected_days} days expected per source)")
    for src in set(d.source for d in daily):
        rows = sum(1 for d in daily if d.source == src)
        print(f"   • {src}: {rows} rows")

    save_records(daily, output_path)
    plot_daily_sentiment(daily)
    return daily


def run_simple_analysis(
    days_back: int = 1,
    output_path: str = "data/processed/daily_sentiment.parquet",
    include_today: bool = True,
) -> List[DailySentiment]:
    """
    Telegram-only simple analysis over a continuous window.
    Uses adapter's time-range fetch and aggregates with full date coverage.
    """
    settings = get_settings()
    start_date, end_date = _calc_window(days_back, include_today)

    tg_adapter = TelegramAdapter()
    coros = []
    for g in settings.TELEGRAM_GROUPS or []:
        coros.append(tg_adapter.fetch_messages_time_range(g, start_date, end_date))
    for ch in settings.TELEGRAM_CHANNELS or []:
        coros.append(tg_adapter.fetch_messages_time_range(ch, start_date, end_date))

    messages = []
    if coros:
        for chunk in _run_in_current_loop(coros):
            messages.extend(chunk)

    # If nothing fetched, still emit a full calendar (so charts show the whole window)
    if not messages:
        print("❌ No messages found for this period.")
        daily = Aggregator.aggregate(
            [],
            start_date=start_date,
            end_date=end_date,
            fill_missing=True,
            neutral_fill=50.0,
            sources=["telegram"],
        )
        save_records(daily, output_path)
        plot_daily_sentiment(daily)
        return daily

    # Sentiment + aggregate
    svc = SentimentService(low_thresh=45, high_thresh=55)
    records = [{"date": m.date, "source": "telegram", "text": m.text} for m in messages]
    anns = svc.annotate([r["text"] for r in records])
    for r, a in zip(records, anns):
        r["norm_score"] = a.norm_score
        r["label"] = a.label

    daily = Aggregator.aggregate(
        records,
        start_date=start_date,
        end_date=end_date,
        fill_missing=True,
        neutral_fill=50.0,
        sources=["telegram"],
    )

    expected_days = (end_date.date() - start_date.date()).days + 1
    got_rows = sum(1 for d in daily if d.source == "telegram")
    print(f"📅 Requested window: {start_date.date()} → {end_date.date()} "
          f"({expected_days} days expected) • telegram rows: {got_rows}")

    save_records(daily, output_path)
    plot_daily_sentiment(daily)
    print(f"✅ Analysis complete! {len(daily)} daily records generated.")
    return daily
