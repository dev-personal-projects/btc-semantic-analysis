from datetime import datetime, timezone
from typing import List, Optional
import asyncio
import time
from telethon import TelegramClient
from telethon.errors import FloodWaitError
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
            connection_retries=5, retry_delay=5,
            flood_sleep_threshold=300  # Auto-sleep for flood waits under 5 minutes (better for groups)
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
            
            # Try to get the entity first to validate access
            entity = await self.client.get_entity(channel)
            entity_type = "supergroup" if hasattr(entity, 'megagroup') and entity.megagroup else "channel"
            print(f"✅ Successfully connected to {entity_type}: {entity.title}")
            
            # For groups, add extra validation
            if hasattr(entity, 'megagroup') and entity.megagroup:
                print(f"📊 Group has {getattr(entity, 'participants_count', 'unknown')} members")
            
            async for m in self.client.iter_messages(entity, limit=limit):
                if since:
                    # Ensure both dates have timezone info for comparison
                    msg_date = m.date.replace(tzinfo=timezone.utc) if m.date.tzinfo is None else m.date
                    since_date = since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since
                    if msg_date < since_date:
                        break
                if m.text and len(m.text.strip()) > 0:  # Filter out empty messages
                    results.append(TelegramMessage(
                        channel=entity.title, id=m.id, date=m.date, text=m.text.strip()
                    ))
        except FloodWaitError as e:
            print(f"⚠️ Rate limit hit for {channel}. Need to wait {e.seconds} seconds.")
            if e.seconds > 600:  # If wait time is more than 10 minutes
                print(f"❌ Wait time too long ({e.seconds}s). Skipping {channel} for now.")
                print("💡 Try again later or reduce the number of messages requested.")
                print("💡 For groups, consider using batch processing with smaller time windows.")
            else:
                print(f"⏳ Waiting {e.seconds} seconds...")
                await asyncio.sleep(e.seconds + 5)  # Add buffer
                # Retry with much smaller limit for groups
                retry_limit = min(limit//3, 100) if limit > 100 else 50
                print(f"🔄 Retrying with reduced limit: {retry_limit}")
                return await self.fetch_messages_for_channel(channel, retry_limit, since)
        except Exception as e:
            print(f"Error fetching from {channel}: {e}")
            if "Cannot find any entity" in str(e):
                print(f"💡 Suggestion: Make sure you're a member of '{channel}' or try a different identifier")
                print(f"💡 For private groups, you must be a member with message history access")
            elif "CHAT_ADMIN_REQUIRED" in str(e):
                print(f"💡 This group requires admin privileges to access message history")
            elif "USER_BANNED_IN_CHANNEL" in str(e):
                print(f"💡 You appear to be banned from this group")
        return results

    async def fetch_messages_time_range(
        self, channel: str, start_date: datetime, end_date: datetime
    ) -> List[TelegramMessage]:
        """Fetch ALL messages within a specific time range"""
        results = []
        try:
            await self.authenticate_if_needed()
            
            entity = await self.client.get_entity(channel)
            entity_type = "supergroup" if hasattr(entity, 'megagroup') and entity.megagroup else "channel"
            print(f"✅ Successfully connected to {entity_type}: {entity.title}")
            
            if hasattr(entity, 'megagroup') and entity.megagroup:
                print(f"📊 Group has {getattr(entity, 'participants_count', 'unknown')} members")
            
            # Ensure dates have timezone info
            start_date_tz = start_date.replace(tzinfo=timezone.utc) if start_date.tzinfo is None else start_date
            end_date_tz = end_date.replace(tzinfo=timezone.utc) if end_date.tzinfo is None else end_date
            
            # Fetch messages within the time range (no limit, get all messages)
            async for m in self.client.iter_messages(
                entity, 
                offset_date=end_date_tz,  # Start from end_date and go backwards
                reverse=False  # Go backwards in time
            ):
                msg_date = m.date.replace(tzinfo=timezone.utc) if m.date.tzinfo is None else m.date
                if msg_date < start_date_tz:
                    break  # Stop when we reach messages older than start_date
                    
                if m.text and len(m.text.strip()) > 0:
                    results.append(TelegramMessage(
                        channel=entity.title, id=m.id, date=m.date, text=m.text.strip()
                    ))
                    
        except FloodWaitError as e:
            print(f"⚠️ Rate limit hit for {channel}. Need to wait {e.seconds} seconds.")
            if e.seconds > 300:  # If wait time is more than 5 minutes
                print(f"❌ Wait time too long ({e.seconds}s). Skipping {channel} for now.")
            else:
                print(f"⏳ Waiting {e.seconds} seconds...")
                await asyncio.sleep(e.seconds + 5)
                # Retry after waiting
                return await self.fetch_messages_time_range(channel, start_date, end_date)
        except Exception as e:
            print(f"Error fetching from {channel}: {e}")
            if "Cannot find any entity" in str(e):
                print(f"💡 Make sure you're a member of '{channel}'")
            elif "CHAT_ADMIN_REQUIRED" in str(e):
                print(f"💡 This group requires admin privileges")
        
        return results
    
    def fetch_time_range(self, channels: List[str], start_date: datetime, 
                        end_date: datetime, is_group: bool = False) -> List[TelegramMessage]:
        """Fetch messages from multiple channels within a time range"""
        async def fetch_all():
            await self.authenticate_if_needed()
            all_messages = []
            
            for i, ch in enumerate(channels):
                entity_type = "group" if is_group else "channel"
                print(f"📥 Fetching from {entity_type} {ch} ({i+1}/{len(channels)})...")
                
                # Add delay between channels to respect rate limits
                if i > 0:
                    delay = 8 if is_group else 3
                    print(f"⏳ Waiting {delay}s to respect rate limits...")
                    await asyncio.sleep(delay)
                
                messages = await self.fetch_messages_time_range(ch, start_date, end_date)
                if messages:
                    print(f"✅ Successfully fetched {len(messages)} messages from {ch}")
                    all_messages.extend(messages)
                else:
                    print(f"⚠️ No messages in time range for {ch}")
                    
            if self.client.is_connected():
                await self.client.disconnect()
            return all_messages
        return asyncio.run(fetch_all())

    def fetch_multiple(self, channels: List[str], limit_per_channel: int = 100, 
                      since: Optional[datetime] = None, is_group: bool = False) -> List[TelegramMessage]:
        async def fetch_all():
            await self.authenticate_if_needed()
            all_messages = []
            print(f"Telegram Groups: {channels}")
            
            for i, ch in enumerate(channels):
                entity_type = "group" if is_group else "channel"
                print(f"📥 Fetching from {entity_type} {ch} ({i+1}/{len(channels)})...")
                
                # Groups need longer delays to avoid rate limits
                delay = 5 if is_group else 2
                if i > 0:
                    print(f"⏳ Waiting {delay}s to respect rate limits...")
                    await asyncio.sleep(delay)
                
                messages = await self.fetch_messages_for_channel(ch, limit_per_channel, since)
                if messages:
                    print(f"✅ Successfully fetched {len(messages)} messages from {ch}")
                    all_messages.extend(messages)
                else:
                    print(f"⚠️ No messages fetched from {ch}")
                    if is_group:
                        print(f"💡 For groups, ensure you're a member and have message history access")
                    
            if self.client.is_connected():
                await self.client.disconnect()
            return all_messages
        return asyncio.run(fetch_all())
