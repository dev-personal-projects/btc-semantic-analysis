from src.btc_sentiment.config.config import get_settings
import os

os.environ["ENVIRONMENT"] = "development"
settings = get_settings()

print("Channels:", settings.TELEGRAM_CHANNELS)
print("Agg Window (minutes):", settings.AGG_WINDOW_MINUTES)
print("Thresholds:", settings.THRESHOLDS)
