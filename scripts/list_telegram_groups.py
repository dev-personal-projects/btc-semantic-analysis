#!/usr/bin/env python3
"""
Helper script to list your Telegram groups and their identifiers.
Run this to find the correct group identifier for your config.
"""

import asyncio
from telethon import TelegramClient
from telethon.tl.types import Chat, Channel
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from btc_sentiment.config.config import get_settings

async def list_groups():
    settings = get_settings()
    
    client = TelegramClient(
        "list_groups.session", 
        settings.TG_API_ID, 
        settings.TG_API_HASH
    )
    
    try:
        await client.start()
        print("🔍 Scanning your Telegram groups and channels...\n")
        
        groups = []
        channels = []
        
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            
            if isinstance(entity, Chat):
                # Regular group
                groups.append({
                    'title': entity.title,
                    'id': entity.id,
                    'identifier': f"-{entity.id}"  # Groups use negative IDs
                })
            elif isinstance(entity, Channel):
                if entity.megagroup:
                    # Supergroup
                    username = f"@{entity.username}" if entity.username else f"-100{entity.id}"
                    groups.append({
                        'title': entity.title,
                        'id': entity.id,
                        'identifier': username
                    })
                else:
                    # Channel
                    username = f"@{entity.username}" if entity.username else f"-100{entity.id}"
                    channels.append({
                        'title': entity.title,
                        'id': entity.id,
                        'identifier': username
                    })
        
        print("📱 GROUPS:")
        print("=" * 50)
        for group in groups:
            print(f"Title: {group['title']}")
            print(f"Identifier: {group['identifier']}")
            print(f"ID: {group['id']}")
            print("-" * 30)
        
        print("\n📢 CHANNELS:")
        print("=" * 50)
        for channel in channels:
            print(f"Title: {channel['title']}")
            print(f"Identifier: {channel['identifier']}")
            print(f"ID: {channel['id']}")
            print("-" * 30)
        
        print("\n💡 Usage:")
        print("Copy the 'Identifier' value to your config.py TELEGRAM_GROUPS or TELEGRAM_CHANNELS list")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(list_groups())