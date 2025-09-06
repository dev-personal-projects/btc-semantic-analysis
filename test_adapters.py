#!/usr/bin/env python3

from src.btc_sentiment.adapters import TelegramAdapter, XApiAdapter
from src.btc_sentiment.config.config import get_settings


def test_adapters():
    """Test adapter initialization"""

    # Test config loading
    try:
        settings = get_settings()
        print("✓ Config loaded successfully")
        print(f"  Channels: {settings.TELEGRAM_CHANNELS}")
    except Exception as e:
        print(f"✗ Config error: {e}")
        return

    # Test X API Adapter (without actual API call)
    try:
        XApiAdapter()
        print("✓ X API Adapter initialized")
    except Exception as e:
        print(f"✗ X API Adapter error: {e}")

    # Test Telegram Adapter (without actual API call)
    try:
        TelegramAdapter()
        print("✓ Telegram Adapter initialized")
    except Exception as e:
        print(f"✗ Telegram Adapter error: {e}")


if __name__ == "__main__":
    test_adapters()
