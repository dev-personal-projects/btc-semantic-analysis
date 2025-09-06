from datetime import datetime, timedelta
from typing import List
from ..config.config import get_settings
from ..adapters.x_api_adapter import XApiAdapter
from ..adapters.telegram_adapter import TelegramAdapter
from ..services.sentiment_service import SentimentService
from ..services.aggregator import Aggregator, DailySentiment
from ..utils.io import save_records
from ..utils.viz import plot_daily_sentiment

def run_ingest_pipeline(
    days_back: int = 1,
    tweets_per_day: int = 200,
    messages_per_channel: int = 200,
    output_path: str = "data/processed/daily_sentiment.parquet"
):
    settings = get_settings()
    since = datetime.utcnow() - timedelta(days=days_back)

    x_adapter = XApiAdapter()
    tweets = x_adapter.fetch_recent_tweets(
        query="(bitcoin OR BTC) lang:en -is:retweet",
        limit=tweets_per_day * days_back,
        start_time=since
    )

    tg_adapter = TelegramAdapter()
    messages = tg_adapter.fetch_multiple(
        settings.TELEGRAM_CHANNELS,
        limit_per_channel=messages_per_channel,
        since=since
    )

    svc = SentimentService(low_thresh=45, high_thresh=55)
    tweet_records = [
        {"date": t.created_at, "source": "twitter", "text": t.text}
        for t in tweets
    ]
    msg_records = [
        {"date": m.date, "source": "telegram", "text": m.text}
        for m in messages
    ]

    for rec_list in (tweet_records, msg_records):
        ann = svc.annotate([r["text"] for r in rec_list])
        for r, a in zip(rec_list, ann):
            r["norm_score"] = a.norm_score
            r["label"] = a.label

    all_records = tweet_records + msg_records
    daily: List[DailySentiment] = Aggregator.aggregate(all_records)
    save_records(daily, output_path)
    plot_daily_sentiment(daily)
    return daily
