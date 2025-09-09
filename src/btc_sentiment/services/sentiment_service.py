from typing import List
from pydantic import BaseModel
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class AnnotatedRecord(BaseModel):
    text: str
    compound: float
    norm_score: float
    label: str


class SentimentService:
    """
    VADER-based sentiment with 0–100 normalization and crisp-neutral at 50:
      - 0..49   -> negative
      -   50    -> neutral
      - 51..100 -> positive
    """
    def __init__(self, low_thresh: float = 50.0, high_thresh: float = 50.0):
        self.analyzer = SentimentIntensityAnalyzer()
        self.low = float(low_thresh)
        self.high = float(high_thresh)

    def _label_from_norm(self, norm: float) -> str:
        if norm < self.low:
            return "negative"
        if norm > self.high:
            return "positive"
        return "neutral"

    def annotate(self, texts: List[str]) -> List[AnnotatedRecord]:
        out: List[AnnotatedRecord] = []
        for t in texts:
            t = (t or "").strip()
            scores = self.analyzer.polarity_scores(t)
            comp = float(scores.get("compound", 0.0))
            # Map [-1, 1] -> [0, 100]
            norm = max(0.0, min(100.0, (comp + 1.0) * 50.0))
            out.append(
                AnnotatedRecord(
                    text=t,
                    compound=comp,
                    norm_score=norm,
                    label=self._label_from_norm(norm),
                )
            )
        return out
