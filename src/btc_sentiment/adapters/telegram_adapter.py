from datetime import datetime, timezone
from typing import List, Optional
import asyncio
from telethon import TelegramClient
from ..models.telegram_message_model import TelegramMessage
from ..config.config import get_settings

class TelegramAdapter:
    def __init__(self, session_name: str = "telegram.session"):
        settings = get_settings()
        self.api_id = settings.TG_API_ID
        self.api_hash = settings.TG_API_HASH
        self.session = session_name
        self.client = TelegramClient(
            self.session, self.api_id, self.api_hash,
            connection_retries=3, retry_delay=1
        )

    async def authenticate_if_needed(self):
        if not self.client.is_connected():
            await self.client.connect()
            
        if not await self.client.is_user_authorized():
            phone = input("Enter your phone number: ")
            await self.client.send_code_request(phone)
            code = input("Enter the code you received: ")
            try:
                await self.client.sign_in(phone, code)
            except Exception:
                password = input("Enter your 2FA password: ")
                await self.client.sign_in(password=password)
    
    async def fetch_messages_for_channel(
        self, channel: str, limit: int = 100, since: Optional[datetime] = None
    ) -> List[TelegramMessage]:
        results = []
        try:
            await self.authenticate_if_needed()
            async for m in self.client.iter_messages(channel, limit=limit):
                if since:
                    msg_date = m.date.replace(tzinfo=timezone.utc) if m.date.tzinfo is None else m.date
                    since_date = since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since
                    if msg_date < since_date:
                        break
                if m.text:
                    results.append(TelegramMessage(
                        channel=channel, id=m.id, date=m.date, text=m.text
                    ))
        except Exception as e:
            print(f"Error fetching from {channel}: {e}")
        return results

    def fetch_multiple(self, channels: List[str], limit_per_channel: int = 100, 
                      since: Optional[datetime] = None) -> List[TelegramMessage]:
        async def fetch_all():
            await self.authenticate_if_needed()
            all_messages = []
            for ch in channels:
                messages = await self.fetch_messages_for_channel(ch, limit_per_channel, since)
                all_messages.extend(messages)
            if self.client.is_connected():
                await self.client.disconnect()
            return all_messages
        return asyncio.run(fetch_all())
