import matplotlib.pyplot as plt
import pandas as pd
from typing import List
from ..services.aggregator import DailySentiment

def plot_daily_sentiment(records: List[DailySentiment]):
    df = pd.DataFrame([r.dict() for r in records])
    df['date'] = pd.to_datetime(df['date'])
    fig, ax = plt.subplots(figsize=(10, 6))
    for source, grp in df.groupby("source"):
        ax.plot(grp["date"], grp["avg_score"], marker="o", label=source)
    ax.set_title("Daily Average Sentiment Score by Source")
    ax.set_xlabel("Date")
    ax.set_ylabel("Sentiment (0–100)")
    ax.legend()
    plt.tight_layout()
    plt.show()
