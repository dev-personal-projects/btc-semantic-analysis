from datetime import datetime, timezone
from typing import List, Optional, Generator
import asyncio
import time
import logging
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, AuthKeyUnregisteredError, SessionPasswordNeededError,
    ChatAdminRequiredError, UserBannedInChannelError, ChannelPrivateError,
    PeerIdInvalidError, UsernameNotOccupiedError, TimeoutError as TelethonTimeoutError
)
from telethon.tl.types import Channel, Chat
from ..models.telegram_message_model import TelegramMessage
from ..config.config import get_settings

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TelegramAdapter:
    def __init__(self, session_name: str = "telegram.session"):
        settings = get_settings()
        self.api_id = settings.TG_API_ID
        self.api_hash = settings.TG_API_HASH
        self.session = session_name
        self._client = None
        self.max_retries = 3
        self.base_delay = 5
        self.authenticated = False
        
    @property
    def client(self) -> TelegramClient:
        """Lazy initialization of client"""
        if self._client is None:
            self._client = TelegramClient(
                self.session, 
                self.api_id, 
                self.api_hash,
                connection_retries=5, 
                retry_delay=5,
                flood_sleep_threshold=60,  # Auto-handle waits up to 1 minute
                timeout=30,  # 30 second timeout for operations
                request_retries=3,
                sequential_updates=True  # Better for stability
            )
        return self._client

    async def authenticate_if_needed(self) -> bool:
        """Enhanced authentication with better session management"""
        try:
            if not self.client.is_connected():
                logger.info("Connecting to Telegram...")
                await self.client.connect()
            
            # Check if we're already authenticated
            if await self.client.is_user_authorized():
                self.authenticated = True
                logger.info("Already authenticated")
                return True
            
            # Need to authenticate
            logger.info("Authentication required")
            phone = input("Enter your phone number (with country code, e.g. +1234567890): ")
            
            try:
                await self.client.send_code_request(phone)
                code = input("Enter the verification code: ")
                await self.client.sign_in(phone, code)
                
            except SessionPasswordNeededError:
                logger.info("2FA password required")
                password = input("Enter your 2FA password: ")
                await self.client.sign_in(password=password)
            
            self.authenticated = True
            logger.info("Authentication successful")
            return True
            
        except AuthKeyUnregisteredError:
            logger.error("Session expired. Please delete the session file and try again.")
            return False
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    async def _get_entity_safely(self, channel: str):
        """Safely get entity with better error handling"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                entity = await self.client.get_entity(channel)
                entity_info = self._get_entity_info(entity)
                logger.info(f"Connected to {entity_info['type']}: {entity_info['title']}")
                return entity
                
            except (PeerIdInvalidError, UsernameNotOccupiedError, ValueError):
                logger.error(f"Cannot find entity '{channel}'. Check the ID/username.")
                return None
            except ChannelPrivateError:
                logger.error(f"Channel '{channel}' is private and you don't have access.")
                return None
            except ChatAdminRequiredError:
                logger.error(f"Admin privileges required for '{channel}'.")
                return None
            except UserBannedInChannelError:
                logger.error(f"You are banned from '{channel}'.")
                return None
            except TelethonTimeoutError:
                if attempt < max_attempts - 1:
                    logger.warning(f"Timeout getting entity, retrying... ({attempt + 1}/{max_attempts})")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Timeout getting entity after {max_attempts} attempts")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error getting entity: {e}")
                return None
        
        return None

    def _get_entity_info(self, entity) -> dict:
        """Get information about the entity"""
        if isinstance(entity, Channel):
            if getattr(entity, 'megagroup', False):
                return {
                    'type': 'supergroup',
                    'title': entity.title,
                    'participants': getattr(entity, 'participants_count', 'unknown')
                }
            else:
                return {
                    'type': 'channel', 
                    'title': entity.title,
                    'participants': getattr(entity, 'participants_count', 'unknown')
                }
        elif isinstance(entity, Chat):
            return {
                'type': 'group',
                'title': entity.title,
                'participants': getattr(entity, 'participants_count', 'unknown')
            }
        else:
            return {'type': 'unknown', 'title': str(entity), 'participants': 'unknown'}

    async def fetch_messages_for_channel(
        self, 
        channel: str, 
        limit: Optional[int] = None, 
        since: Optional[datetime] = None,
        batch_size: int = 100
    ) -> List[TelegramMessage]:
        """
        Enhanced message fetching with better memory management and error handling
        """
        if not await self.authenticate_if_needed():
            return []

        entity = await self._get_entity_safely(channel)
        if not entity:
            return []

        results = []
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                # Process messages in batches to manage memory
                async for batch in self._fetch_messages_in_batches(entity, limit, since, batch_size):
                    results.extend(batch)
                    
                    # Add small delay between batches to be gentle on the API
                    if len(batch) == batch_size:  # Full batch, likely more coming
                        await asyncio.sleep(0.5)
                
                logger.info(f"Successfully fetched {len(results)} messages from {entity.title}")
                return results

            except FloodWaitError as e:
                retry_count += 1
                wait_time = e.seconds
                
                if wait_time > 600:  # More than 10 minutes
                    logger.error(f"Flood wait too long ({wait_time}s). Skipping {channel}.")
                    break
                elif retry_count < self.max_retries:
                    logger.warning(f"Rate limited. Waiting {wait_time}s... (attempt {retry_count}/{self.max_retries})")
                    await asyncio.sleep(wait_time + 5)
                    # Reduce batch size on retry
                    batch_size = max(batch_size // 2, 10)
                else:
                    logger.error(f"Max retries reached for {channel}")
                    break
                    
            except Exception as e:
                retry_count += 1
                logger.error(f"Error fetching from {channel}: {e}")
                
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.base_delay * retry_count)
                else:
                    break
        
        return results

    async def _fetch_messages_in_batches(
        self, 
        entity, 
        limit: Optional[int], 
        since: Optional[datetime], 
        batch_size: int
    ) -> Generator[List[TelegramMessage], None, None]: # type: ignore
        """Generator that yields batches of messages to manage memory"""
        batch = []
        processed = 0
        
        try:
            async for message in self.client.iter_messages(entity, limit=limit):
                # Check date filter
                if since and self._is_message_too_old(message, since):
                    break
                
                # Only process messages with text
                if message.text and message.text.strip():
                    telegram_msg = TelegramMessage(
                        channel=entity.title,
                        id=message.id,
                        date=message.date.replace(tzinfo=timezone.utc) if message.date.tzinfo is None else message.date,
                        text=message.text.strip()
                    )
                    batch.append(telegram_msg)
                
                processed += 1
                
                # Yield batch when it's full
                if len(batch) >= batch_size:
                    logger.debug(f"Yielding batch of {len(batch)} messages (processed {processed} total)")
                    yield batch
                    batch = []
                
                # Optional: Progress logging for large operations
                if processed % 500 == 0:
                    logger.info(f"Processed {processed} messages...")
            
            # Yield remaining messages
            if batch:
                logger.debug(f"Yielding final batch of {len(batch)} messages")
                yield batch
                
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            if batch:  # Return what we have
                yield batch
            raise

    def _is_message_too_old(self, message, since: datetime) -> bool:
        """Check if message is older than the since date"""
        msg_date = message.date.replace(tzinfo=timezone.utc) if message.date.tzinfo is None else message.date
        since_date = since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since
        return msg_date < since_date

    async def fetch_messages_time_range(
        self, 
        channel: str, 
        start_date: datetime, 
        end_date: datetime,
        batch_size: int = 100
    ) -> List[TelegramMessage]:
        """Enhanced time range fetching with better memory management"""
        if not await self.authenticate_if_needed():
            return []

        entity = await self._get_entity_safely(channel)
        if not entity:
            return []

        # Ensure dates have timezone info
        start_date_tz = start_date.replace(tzinfo=timezone.utc) if start_date.tzinfo is None else start_date
        end_date_tz = end_date.replace(tzinfo=timezone.utc) if end_date.tzinfo is None else end_date

        results = []
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                async for batch in self._fetch_time_range_batches(entity, start_date_tz, end_date_tz, batch_size):
                    results.extend(batch)
                    await asyncio.sleep(0.5)  # Be gentle on API
                
                logger.info(f"Fetched {len(results)} messages from {entity.title} in time range")
                return results

            except FloodWaitError as e:
                retry_count += 1
                if e.seconds <= 300 and retry_count < self.max_retries:
                    logger.warning(f"Waiting {e.seconds}s for rate limit... (attempt {retry_count})")
                    await asyncio.sleep(e.seconds + 5)
                    batch_size = max(batch_size // 2, 10)  # Reduce batch size
                else:
                    logger.error(f"Rate limit too long or max retries reached")
                    break
                    
            except Exception as e:
                retry_count += 1
                logger.error(f"Error in time range fetch: {e}")
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.base_delay * retry_count)
                else:
                    break
        
        return results

    async def _fetch_time_range_batches(
        self, 
        entity, 
        start_date: datetime, 
        end_date: datetime, 
        batch_size: int
    ) -> Generator[List[TelegramMessage], None, None]: # type: ignore
        """Fetch messages in time range using batches"""
        batch = []
        
        async for message in self.client.iter_messages(entity, offset_date=end_date, reverse=False):
            msg_date = message.date.replace(tzinfo=timezone.utc) if message.date.tzinfo is None else message.date
            
            if msg_date < start_date:
                break
                
            if message.text and message.text.strip():
                telegram_msg = TelegramMessage(
                    channel=entity.title,
                    id=message.id,
                    date=msg_date,
                    text=message.text.strip()
                )
                batch.append(telegram_msg)
                
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        
        if batch:
            yield batch

    def fetch_time_range(
        self, 
        channels: List[str], 
        start_date: datetime, 
        end_date: datetime, 
        is_group: bool = False
    ) -> List[TelegramMessage]:
        """Synchronous wrapper with improved error handling"""
        async def fetch_all():
            all_messages = []
            
            for i, channel in enumerate(channels):
                entity_type = "group" if is_group else "channel"
                logger.info(f"Fetching from {entity_type} {channel} ({i+1}/{len(channels)})...")
                
                # Progressive delays to avoid rate limits
                if i > 0:
                    delay = 8 if is_group else 3
                    logger.info(f"Waiting {delay}s to respect rate limits...")
                    await asyncio.sleep(delay)
                
                try:
                    messages = await self.fetch_messages_time_range(channel, start_date, end_date)
                    if messages:
                        logger.info(f"Successfully fetched {len(messages)} messages from {channel}")
                        all_messages.extend(messages)
                    else:
                        logger.warning(f"No messages found for {channel}")
                        
                except Exception as e:
                    logger.error(f"Failed to fetch from {channel}: {e}")
                    continue
            
            return all_messages
        
        try:
            return asyncio.run(fetch_all())
        except Exception as e:
            logger.error(f"Error in fetch_time_range: {e}")
            return []
        finally:
            # Ensure client is disconnected
            if self._client and self._client.is_connected():
                asyncio.run(self._client.disconnect())

    def fetch_multiple(
        self, 
        channels: List[str], 
        limit_per_channel: int = 100, 
        since: Optional[datetime] = None, 
        is_group: bool = False
    ) -> List[TelegramMessage]:
        """Enhanced multiple channel fetching"""
        async def fetch_all():
            all_messages = []
            logger.info(f"Fetching from {len(channels)} {'groups' if is_group else 'channels'}")
            
            for i, channel in enumerate(channels):
                entity_type = "group" if is_group else "channel"
                logger.info(f"Processing {entity_type} {channel} ({i+1}/{len(channels)})...")
                
                # Staggered delays based on entity type
                if i > 0:
                    delay = 8 if is_group else 3
                    logger.info(f"Waiting {delay}s to respect rate limits...")
                    await asyncio.sleep(delay)
                
                try:
                    messages = await self.fetch_messages_for_channel(channel, limit_per_channel, since)
                    if messages:
                        logger.info(f"Fetched {len(messages)} messages from {channel}")
                        all_messages.extend(messages)
                    else:
                        logger.warning(f"No messages retrieved from {channel}")
                        
                except Exception as e:
                    logger.error(f"Error processing {channel}: {e}")
                    continue
            
            logger.info(f"Total messages collected: {len(all_messages)}")
            return all_messages
        
        try:
            return asyncio.run(fetch_all())
        except Exception as e:
            logger.error(f"Error in fetch_multiple: {e}")
            return []
        finally:
            # Clean up connection
            if self._client and self._client.is_connected():
                asyncio.run(self._client.disconnect())

    async def disconnect(self):
        """Properly disconnect the client"""
        if self._client and self._client.is_connected():
            await self._client.disconnect()
            logger.info("Telegram client disconnected")

    def __del__(self):
        """Cleanup on object destruction"""
        if self._client and self._client.is_connected():
            try:
                asyncio.run(self._client.disconnect())
            except:
                pass  # Ignore errors during cleanup