

from typing import List, Dict
from pydantic import BaseModel
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class AnnotatedRecord(BaseModel):
    text: str
    compound: float
    norm_score: float
    label: str


class SentimentService:
    def __init__(self, low_thresh: int = 45, high_thresh: int = 55):
        self.analyzer = SentimentIntensityAnalyzer()
        self.low = low_thresh
        self.high = high_thresh

    def annotate(self, texts: List[str]) -> List[AnnotatedRecord]:
        results = []
        for text in texts:
            comp = self.analyzer.polarity_scores(text)["compound"]
            norm = (comp + 1) * 50
            if norm <= self.low:
                lbl = "negative"
            elif norm >= self.high:
                lbl = "positive"
            else:
                lbl = "neutral"
            results.append(AnnotatedRecord(text=text, compound=comp, norm_score=norm, label=lbl))
        return results
