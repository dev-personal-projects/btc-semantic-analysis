# src/btc_sentiment/adapters/telegram_adapter.py

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional, Union

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from ..config.config import get_settings
from ..models.telegram_message_model import TelegramMessage

logger = logging.getLogger(__name__)


class TelegramAdapter:
    def __init__(self, session_name: str = "telegram.session"):
        s = get_settings()
        self.api_id = s.TG_API_ID
        self.api_hash = s.TG_API_HASH
        self.session = session_name
        self._client: Optional[TelegramClient] = None

    @property
    def client(self) -> TelegramClient:
        if self._client is None:
            self._client = TelegramClient(
                self.session,
                self.api_id,
                self.api_hash,
                connection_retries=5,
                retry_delay=5,
                flood_sleep_threshold=60,
                timeout=30,
                request_retries=3,
                sequential_updates=True,
            )
        return self._client

    async def authenticate_if_needed(self) -> None:
        if not self.client.is_connected():
            await self.client.connect()
        if not await self.client.is_user_authorized():
            phone = input("Enter phone (e.g. +1234567890): ")
            await self.client.send_code_request(phone)
            try:
                code = input("Enter the verification code: ")
                await self.client.sign_in(phone, code)
            except Exception:
                password = input("Enter your 2FA password: ")
                await self.client.sign_in(password=password)

    async def _get_entity(self, channel: Union[str, int]):
        try:
            return await self.client.get_entity(channel)
        except Exception as e:
            logger.error(f"Could not resolve entity '{channel}': {e}")
            return None

    @staticmethod
    def _to_utc(dt: datetime) -> datetime:
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    async def fetch_messages_for_channel(
        self,
        channel: Union[str, int],
        limit: Optional[int] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[TelegramMessage]:
        results: List[TelegramMessage] = []

        await self.authenticate_if_needed()
        entity = await self._get_entity(channel)
        if not entity:
            return results

        if since:
            since = self._to_utc(since)
        if until:
            until = self._to_utc(until)

        try:
            # reverse=True => oldest -> newest
            # offset_date=since starts just AFTER the lower bound (exclusive)
            async for m in self.client.iter_messages(
                entity,
                reverse=True,
                offset_date=since,
                limit=limit,
            ):
                if not m.text:
                    continue
                msg_dt = self._to_utc(m.date)
                if until and msg_dt > until:
                    break
                results.append(
                    TelegramMessage(
                        channel=getattr(entity, "title", str(channel)),
                        id=m.id,
                        date=msg_dt,
                        text=m.text.strip(),
                    )
                )
        except FloodWaitError as e:
            if e.seconds <= 300:
                await asyncio.sleep(e.seconds + 5)
                return await self.fetch_messages_for_channel(
                    channel=channel, limit=limit, since=since, until=until
                )
            logger.error(f"Flood wait {e.seconds}s for {channel}. Skipping.")
        except Exception as e:
            logger.error(f"Error fetching from {channel}: {e}")

        return results

    async def fetch_messages_time_range(
        self,
        channel: Union[str, int],
        start_date: datetime,
        end_date: datetime,
    ) -> List[TelegramMessage]:
        return await self.fetch_messages_for_channel(
            channel=channel,
            limit=None,
            since=self._to_utc(start_date),
            until=self._to_utc(end_date),
        )

    def fetch_multiple(
        self,
        channels: List[Union[str, int]],
        limit_per_channel: int = 100,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[TelegramMessage]:
        async def run():
            msgs: List[TelegramMessage] = []
            await self.authenticate_if_needed()
            for ch in channels:
                part = await self.fetch_messages_for_channel(
                    channel=ch,
                    limit=limit_per_channel,
                    since=since,
                    until=until,
                )
                msgs.extend(part)
                await asyncio.sleep(2)
            if self._client and self._client.is_connected():
                await self._client.disconnect()
            return msgs

        try:
            return asyncio.run(run())
        except Exception as e:
            logger.error(f"fetch_multiple failed: {e}")
            return []

    def fetch_time_range(
        self,
        channels: List[Union[str, int]],
        start_date: datetime,
        end_date: datetime,
    ) -> List[TelegramMessage]:
        async def run():
            msgs: List[TelegramMessage] = []
            await self.authenticate_if_needed()
            for ch in channels:
                part = await self.fetch_messages_time_range(
                    channel=ch, start_date=start_date, end_date=end_date
                )
                msgs.extend(part)
                await asyncio.sleep(2)
            if self._client and self._client.is_connected():
                await self._client.disconnect()
            return msgs

        try:
            return asyncio.run(run())
        except Exception as e:
            logger.error(f"fetch_time_range failed: {e}")
            return []
