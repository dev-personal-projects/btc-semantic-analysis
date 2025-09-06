

from datetime import datetime
from typing import List, Optional

import tweepy
from pydantic import BaseModel, Field

from ..config.config import get_settings


class TweetData(BaseModel):
    id: int
    created_at: datetime
    text: str


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
        limit: int = 500,
        start_time: Optional[datetime] = None,
    ) -> List[TweetData]:
        paginator = tweepy.Paginator(
            self.client.search_recent_tweets,
            query=query,
            tweet_fields="created_at",
            start_time=start_time,
            max_results=100,
        )
        tweets = []
        try:
            for tweet in paginator.flatten(limit=limit):
                tweets.append(TweetData(
                    id=tweet.id,
                    created_at=tweet.created_at,
                    text=tweet.text,
                ))
        except Exception as e:
            print(f"Error fetching tweets: {e}")
        return tweets
