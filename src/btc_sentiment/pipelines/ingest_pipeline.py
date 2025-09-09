from datetime import datetime, timedelta
from typing import List
from ..config.config import get_settings
from ..adapters.x_api_adapter import XApiAdapter
from ..adapters.telegram_adapter import TelegramAdapter
from ..services.sentiment_service import SentimentService
from ..services.aggregator import Aggregator, DailySentiment
from ..utils.io import save_records
from ..utils.visualizations import plot_daily_sentiment

def run_ingest_pipeline(
    days_back: int = 1,
    tweets_per_day: int = 50,
    messages_limit: int = 200,
    output_path: str = "data/processed/daily_sentiment.parquet"
):
    from datetime import timezone
    settings = get_settings()
    since = datetime.now(timezone.utc) - timedelta(days=days_back)

    x_adapter = XApiAdapter()
    tweets = x_adapter.fetch_recent_tweets(
        query="(bitcoin OR BTC) lang:en -is:retweet",
        limit=tweets_per_day * days_back,
        start_time=since
    )

    tg_adapter = TelegramAdapter()
    messages = []
    
    # Fetch from channels if configured
    if settings.TELEGRAM_CHANNELS:
        print(f"Fetching from {len(settings.TELEGRAM_CHANNELS)} channels...")
        channel_messages = tg_adapter.fetch_multiple(
            settings.TELEGRAM_CHANNELS,
            limit_per_channel=messages_limit,
            since=since
        )
        messages.extend(channel_messages)
    
    # Fetch from groups if configured
    if settings.TELEGRAM_GROUPS:
        print(f"Fetching from {len(settings.TELEGRAM_GROUPS)} groups...")
        group_messages = tg_adapter.fetch_multiple(
            settings.TELEGRAM_GROUPS,
            limit_per_channel=messages_limit,
            since=since
        )
        messages.extend(group_messages)
    
    print(f"Total Telegram messages fetched: {len(messages)}")

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
    
    if not all_records:
        print("❌ No data fetched. Possible issues:")
        print("   - Check your API credentials in .env file")
        print("   - Verify group/channel names or usernames")
        print("   - Ensure you have access to the groups/channels")
        print("   - For private groups, you must be a member")
        return []
    
    daily: List[DailySentiment] = Aggregator.aggregate(all_records)
    save_records(daily, output_path)
    plot_daily_sentiment(daily)
    return daily

def run_telegram_only_pipeline(
    days_back: int = 1,
    messages_per_channel: int = 1000,
    output_path: str = "data/processed/daily_sentiment.parquet"
):
    """Legacy pipeline - use run_time_based_analysis for better results"""
    print("⚠️ This function is deprecated. Use run_time_based_analysis() instead.")
    return run_time_based_analysis(days_back, 7, output_path)

def run_simple_analysis(
    days_back: int = 1,
    output_path: str = "data/processed/daily_sentiment.parquet"
):
    """Simple analysis for a specific time period - no batches, just get the data"""
    from datetime import timezone
    settings = get_settings()
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    
    print(f"📈 Running simple analysis for {days_back} day(s)")
    print(f"📅 Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    if not settings.TELEGRAM_GROUPS:
        print("⚠️ No Telegram groups configured")
        return []
    
    tg_adapter = TelegramAdapter()
    messages = tg_adapter.fetch_time_range(
        settings.TELEGRAM_GROUPS,
        start_date=start_date,
        end_date=end_date,
        is_group=True
    )
    
    if not messages:
        print("❌ No messages found for this time period")
        return []
    
    print(f"📊 Analyzing {len(messages)} messages...")
    svc = SentimentService(low_thresh=45, high_thresh=55)
    records = [{"date": m.date, "source": "telegram", "text": m.text} for m in messages]
    
    ann = svc.annotate([r["text"] for r in records])
    for r, a in zip(records, ann):
        r["norm_score"] = a.norm_score
        r["label"] = a.label
    
    daily: List[DailySentiment] = Aggregator.aggregate(records)
    save_records(daily, output_path)
    plot_daily_sentiment(daily)
    
    print(f"✅ Analysis complete! {len(daily)} daily records generated")
    return daily


