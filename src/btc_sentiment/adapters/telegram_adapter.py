"""
telegram_adapter.py
-------------------
Connects to Telegram via Telethon to fetch messages from public channels asynchronously.

Features:
- Fetches historical messages via `iter_messages`, filtering by date if needed.
- Handles multiple channels efficiently.
- Returns structured data using a Pydantic model.

Sources & Practices:
- `TelegramClient.iter_messages` is the recommended method to fetch chat history from channels or groups. ([turn0search4])
- Use manual date checks when relying on `offset_date`, as there are known inconsistencies. ([turn0search6])
"""

from datetime import datetime
from typing import List, Optional

from src.btc_sentiment.models.telegram_message_model import TelegramMessage
from telethon import TelegramClient
from pydantic import BaseModel

from ..config.config import get_settings




class TelegramAdapter:
    """
    Adapter to fetch messages from Telegram public channels using Telethon.
    """

    def __init__(self, session_name: str = "telegram.session"):
        settings = get_settings()
        self.api_id = settings.TG_API_ID
        self.api_hash = settings.TG_API_HASH
        self.session = session_name
        self.client = TelegramClient(
            self.session, self.api_id, self.api_hash
        )

    async def fetch_messages_for_channel(
        self, channel: str, limit: int = 100, since: Optional[datetime] = None
    ) -> List[TelegramMessage]:
        """
        Fetch messages for a single channel.
        """
        results = []
        try:
            async with self.client:
                async for m in self.client.iter_messages(channel, limit=limit):
                    if since and m.date < since:
                        break
                    if m.text:
                        results.append(
                            TelegramMessage(
                                channel=channel, id=m.id, date=m.date, text=m.text
                            )
                        )
        except Exception as e:
            print(f"Error fetching from {channel}: {e}")
        return results

    def fetch_multiple(
        self,
        channels: List[str],
        limit_per_channel: int = 100,
        since: Optional[datetime] = None,
    ) -> List[TelegramMessage]:
        """
        Synchronously fetch messages across multiple channels.

        Args:
            channels: List of channel usernames.
            limit_per_channel: Number of messages per channel.
            since: Optional datetime to filter results per channel.

        Returns:
            Combined list of TelegramMessage instances from all channels.
        """
        import asyncio

        tasks = [
            self.fetch_messages_for_channel(ch, limit_per_channel, since)
            for ch in channels
        ]
        all_messages = asyncio.run(asyncio.gather(*tasks))
        # Flatten and return
        return [msg for sublist in all_messages for msg in sublist]
