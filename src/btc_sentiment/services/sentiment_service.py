from typing import List
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
        self.low = float(low_thresh)
        self.high = float(high_thresh)

    def _label_from_norm(self, norm: float) -> str:
        if norm <= self.low:
            return "negative"
        if norm >= self.high:
            return "positive"
        return "neutral"

    def annotate(self, texts: List[str]) -> List[AnnotatedRecord]:
        out: List[AnnotatedRecord] = []
        for t in texts:
            t = (t or "").strip()
            scores = self.analyzer.polarity_scores(t)
            comp = float(scores.get("compound", 0.0))
            norm = max(0.0, min(100.0, (comp + 1.0) * 50.0))  # clamp to [0,100]
            out.append(
                AnnotatedRecord(
                    text=t,
                    compound=comp,
                    norm_score=norm,
                    label=self._label_from_norm(norm),
                )
            )
        return out
