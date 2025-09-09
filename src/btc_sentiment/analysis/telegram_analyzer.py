"""
Telegram Groups Sentiment Analysis Module
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import asyncio

from ..adapters.telegram_adapter import TelegramAdapter
from ..services.sentiment_service import SentimentService


class TelegramGroupAnalyzer:
    """Analyzer for Telegram group sentiment analysis"""
    
    def __init__(self, low_thresh: int = 45, high_thresh: int = 55):
        self.sentiment_service = SentimentService(
            low_thresh=low_thresh, 
            high_thresh=high_thresh
        )
        self.tg_adapter = TelegramAdapter()
    
    async def analyze_single_group(self, group_name: str, days_back: int = 7, limit: int = 500) -> Optional[pd.DataFrame]:
        """Analyze sentiment for a single Telegram group"""
        print(f"Analyzing group: {group_name}")
        
        # Fetch messages
        since = datetime.utcnow() - timedelta(days=days_back)
        messages = await self.tg_adapter.fetch_messages_for_channel(group_name, limit, since)
        
        if not messages:
            print(f"No messages found for {group_name}")
            return None
        
        print(f"Found {len(messages)} messages")
        
        # Prepare data for sentiment analysis
        records = []
        for msg in messages:
            records.append({
                "date": msg.date,
                "source": "telegram_group",
                "group": group_name,
                "text": msg.text,
                "message_id": msg.id
            })
        
        # Analyze sentiment
        texts = [r["text"] for r in records]
        sentiments = self.sentiment_service.annotate(texts)
        
        # Add sentiment scores to records
        for record, sentiment in zip(records, sentiments):
            record["norm_score"] = sentiment.norm_score
            record["label"] = sentiment.label
        
        return pd.DataFrame(records)
    
    async def analyze_multiple_groups(self, groups: List[str], days_back: int = 7, limit: int = 500) -> pd.DataFrame:
        """Analyze sentiment across multiple Telegram groups"""
        print(f"Analyzing {len(groups)} groups...")
        
        all_data = []
        for group in groups:
            try:
                group_df = await self.analyze_single_group(group, days_back, limit)
                if group_df is not None:
                    all_data.append(group_df)
            except Exception as e:
                print(f"Error analyzing {group}: {e}")
                continue
        
        if not all_data:
            print("No data collected from any groups")
            return pd.DataFrame()
        
        # Combine all group data
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"Total messages analyzed: {len(combined_df)}")
        
        return combined_df
    
    def get_group_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get summary statistics by group"""
        if df.empty:
            return pd.DataFrame()
        
        summary = df.groupby('group').agg({
            'norm_score': ['mean', 'std', 'count'],
            'label': lambda x: (x == 'positive').sum() / len(x) * 100
        }).round(2)
        
        summary.columns = ['avg_sentiment', 'sentiment_std', 'message_count', 'positive_pct']
        return summary.reset_index()