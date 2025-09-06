"""
x_api_adapter.py
----------------
Provides a connector to Twitter (X) via Tweepy v2 API for fetching recent BTC-related tweets.

Key Features:
- Uses Tweepy Client.search_recent_tweets to query recent tweets (last 7 days).
- Supports fetching tweet metadata via tweet_fields and expansions.
- Handles pagination using tweepy.Paginator with limit support.

Data Structures:
- Returns List[dict] with keys: id, text, created_at, like_count, retweet_count,
  reply_count, quote_count, and references to replies/retweets.

Algorithm:
1. Initialize client with bearer token.
2. Run search with optional query, date range, and metadata request.
3. Use Paginator to fetch up to a user-defined number of tweets.
4. Normalize results into Python dicts.
"""

from datetime import datetime
from typing import List, Optional

import tweepy
from pydantic import BaseModel, Field

from ..config.config import get_settings


class TweetData(BaseModel):
    id: int
    created_at: datetime
    text: str
    like_count: int = Field(..., alias="like_count")
    retweet_count: int = Field(..., alias="retweet_count")
    reply_count: int = Field(..., alias="reply_count")
    quote_count: int = Field(..., alias="quote_count")
    in_reply_to_user_id: Optional[int]
    referenced_tweets: Optional[List[dict]]


class XApiAdapter:
    def __init__(self, bearer_token: Optional[str] = None):
        if bearer_token:
            bearer = bearer_token
        else:
            settings = get_settings()
            bearer = settings.X_BEARER_TOKEN
        
        self.client = tweepy.Client(
            bearer_token=bearer, wait_on_rate_limit=True
        )

    def fetch_recent_tweets(
        self,
        query: str,
        max_results: int = 100,
        limit: int = 500,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[TweetData]:
        """Fetch recent tweets using Tweepy v2 search."""
        tweet_fields = [
            "created_at",
            "public_metrics",
            "in_reply_to_user_id",
            "referenced_tweets",
        ]
        paginator = tweepy.Paginator(
            self.client.search_recent_tweets,
            query=query,
            tweet_fields=",".join(tweet_fields),
            start_time=start_time,
            end_time=end_time,
            max_results=max_results,
        )
        tweets = []
        try:
            for tweet in paginator.flatten(limit=limit):
                pm = tweet.public_metrics or {}
                tweets.append(TweetData(
                    id=tweet.id,
                    created_at=tweet.created_at,
                    text=tweet.text,
                    like_count=pm.get("like_count", 0),
                    retweet_count=pm.get("retweet_count", 0),
                    reply_count=pm.get("reply_count", 0),
                    quote_count=pm.get("quote_count", 0),
                    in_reply_to_user_id=getattr(tweet, "in_reply_to_user_id", None),
                    referenced_tweets=getattr(tweet, "referenced_tweets", None),
                ))
        except Exception as e:
            print(f"Error fetching tweets: {e}")
        return tweets
