from datetime import datetime
from typing import List, Dict
import pandas as pd
from pydantic import BaseModel

class DailySentiment(BaseModel):
    date: datetime
    source: str
    avg_score: float
    count: int
    label: str

class Aggregator:
    @staticmethod
    def aggregate(records: List[Dict]) -> List[DailySentiment]:
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date']).dt.date
        results = []
        for (source, date), grp in df.groupby(['source', 'date']):
            avg = grp['norm_score'].mean()
            cnt = len(grp)
            label = grp['label'].mode().iloc[0] if not grp['label'].mode().empty else "neutral"
            results.append(DailySentiment(date=date, source=source, avg_score=avg, count=cnt, label=label))
        return results
