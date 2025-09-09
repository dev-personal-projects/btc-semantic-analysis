import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Union

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from ..config.config import get_settings
from ..models.telegram_message_model import TelegramMessage

logger = logging.getLogger(__name__)


class TelegramAdapter:
    """
    Telegram fetcher with:
      - paging/batching across long ranges
      - inclusive lower bound (Telethon offsets are exclusive)
      - flood-wait aware retries
    """
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
        limit: Optional[int] = None,            # total cap per channel (None = all)
        since: Optional[datetime] = None,       # inclusive (we make it inclusive)
        until: Optional[datetime] = None,       # inclusive upper bound
        page_size: int = 200,                   # RPC page size
        sleep_between_pages: float = 1.0,
    ) -> List[TelegramMessage]:
        """
        Fetch messages in ascending order (oldest -> newest) between [since, until],
        honoring a total limit, paging through results reliably.
        """
        results: List[TelegramMessage] = []

        await self.authenticate_if_needed()
        entity = await self._get_entity(channel)
        if not entity:
            return results

        # Normalize bounds; make lower bound effectively inclusive (offsets are exclusive).
        since_utc = self._to_utc(since) if since else None
        until_utc = self._to_utc(until) if until else None
        cursor_date = (since_utc - timedelta(seconds=1)) if since_utc else None
        last_id: Optional[int] = None

        def remaining_total() -> Optional[int]:
            if limit is None:
                return None
            return max(0, limit - len(results))

        while True:
            try:
                per_page = page_size
                rem = remaining_total()
                if rem is not None:
                    if rem == 0:
                        break
                    per_page = min(page_size, rem)

                batch_count = 0
                last_dt_this_batch: Optional[datetime] = None

                # Build kwargs to avoid passing None for offsets (fixes int > None errors)
                kwargs = dict(entity=entity, reverse=True, limit=per_page)
                if cursor_date is not None:
                    kwargs["offset_date"] = cursor_date
                if last_id is not None:
                    kwargs["offset_id"] = last_id

                async for m in self.client.iter_messages(**kwargs):
                    if not m.text:
                        continue
                    msg_dt = self._to_utc(m.date)

                    # Respect upper bound (inclusive)
                    if until_utc and msg_dt > until_utc:
                        batch_count = 0  # stop outer loop
                        break

                    results.append(
                        TelegramMessage(
                            channel=getattr(entity, "title", str(channel)),
                            id=m.id,
                            date=msg_dt,
                            text=(m.text or "").strip(),
                        )
                    )
                    batch_count += 1
                    last_id = m.id
                    last_dt_this_batch = msg_dt

                if batch_count == 0:
                    break  # no more data or we hit 'until'

                # Advance cursor (offsets are exclusive; this is safe).
                cursor_date = last_dt_this_batch
                await asyncio.sleep(sleep_between_pages)

            except FloodWaitError as e:
                wait_s = int(e.seconds) + 5
                if wait_s <= 300:
                    logger.warning(f"FloodWait {e.seconds}s on {channel}. Sleeping {wait_s}s, then resuming.")
                    await asyncio.sleep(wait_s)
                    continue
                logger.error(f"FloodWait {e.seconds}s too long on {channel}. Aborting this channel.")
                break
            except Exception as e:
                logger.error(f"Error fetching from {channel}: {e}")
                break

        # Completeness hints
        if since_utc and results:
            earliest = min(r.date for r in results)
            if earliest > since_utc + timedelta(seconds=1):
                logger.warning(
                    f"Data incomplete for {channel}: earliest fetched {earliest.isoformat()} "
                    f"is later than requested start {since_utc.isoformat()}."
                )
        if limit is not None and len(results) == limit:
            logger.warning(
                f"Data may be clipped for {channel}: exactly hit limit={limit} messages."
            )

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
        limit_per_channel: Optional[int] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        page_size: int = 200,
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
                    page_size=page_size,
                )
                msgs.extend(part)
                await asyncio.sleep(1.0)
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
        page_size: int = 200,
    ) -> List[TelegramMessage]:
        async def run():
            msgs: List[TelegramMessage] = []
            await self.authenticate_if_needed()
            for ch in channels:
                part = await self.fetch_messages_time_range(
                    channel=ch, start_date=start_date, end_date=end_date
                )
                msgs.extend(part)
                await asyncio.sleep(1.0)
            if self._client and self._client.is_connected():
                await self._client.disconnect()
            return msgs

        try:
            return asyncio.run(run())
        except Exception as e:
            logger.error(f"fetch_time_range failed: {e}")
            return []
