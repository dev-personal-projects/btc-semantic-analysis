import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import List
from ..services.aggregator import DailySentiment

# Clean defaults
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def plot_daily_sentiment(records: List[DailySentiment], show_no_data_markers: bool = True):
    """
    Daily sentiment plot that:
      - does NOT draw lines across no-data days (gaps via NaN)
      - optionally marks no-data days at y=50 with 'x'
    """
    if not records:
        print("No data available for visualization")
        return

    df = pd.DataFrame([r.dict() for r in records])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['source', 'date'])

    fig, ax = plt.subplots(figsize=(10, 6))

    for source, grp in df.groupby("source"):
        # Break line across no-data (count == 0 or label == "no_data")
        y = grp["avg_score"].where(~((grp["count"] == 0) | (grp["label"] == "no_data")), np.nan)
        ax.plot(grp["date"], y, marker="o", label=source)

        if show_no_data_markers:
            nd = grp[(grp["count"] == 0) | (grp["label"] == "no_data")]
            if not nd.empty:
                ax.scatter(nd["date"], [50.0] * len(nd), marker="x", alpha=0.6, label=f"{source} (no data)")

    ax.axhline(50, linestyle="--", linewidth=1, alpha=0.6, color="gray")  # neutral guide
    ax.set_title("Daily Average Sentiment Score by Source")
    ax.set_xlabel("Date")
    ax.set_ylabel("Sentiment (0–100)")
    ax.legend()
    plt.tight_layout()
    plt.show()


def create_sentiment_dashboard(records: List[DailySentiment]):
    """Comprehensive dashboard; respects no-data gaps in the main trend."""
    if not records:
        print("No data available for visualization")
        return

    df = pd.DataFrame([r.dict() for r in records])
    # dtype safety
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["avg_score"] = pd.to_numeric(df["avg_score"], errors="coerce")

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Bitcoin Sentiment Analysis Dashboard', fontsize=16, fontweight='bold')

    # 1) Daily sentiment trend (don’t draw through no-data)
    for source in df['source'].unique():
        sd = df[df['source'] == source].sort_values('date')
        y = sd["avg_score"].where(~((sd["count"] == 0) | (sd["label"] == "no_data")), np.nan)
        axes[0, 0].plot(sd['date'], y, marker='o', linewidth=2, label=source, alpha=0.85)
    axes[0, 0].axhline(50, linestyle="--", linewidth=1, alpha=0.6, color="gray")
    axes[0, 0].set_title('Daily Sentiment Trend')
    axes[0, 0].set_xlabel('Date')
    axes[0, 0].set_ylabel('Average Sentiment Score')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # 2) Sentiment distribution by source (FIXED operator precedence)
    sentiment_data, labels = [], []
    for source in df['source'].unique():
        mask = (df['source'] == source) & (df['count'] > 0)
        sentiment_data.append(df.loc[mask, 'avg_score'])
        labels.append(source)
    axes[0, 1].boxplot(sentiment_data, labels=labels)
    axes[0, 1].set_title('Sentiment Score Distribution by Source')
    axes[0, 1].set_ylabel('Sentiment Score')

    # 3) Message volume by source
    source_counts = df.groupby('source')['count'].sum()
    colors = plt.cm.Set3(np.linspace(0, 1, len(source_counts)))
    axes[0, 2].pie(source_counts.values, labels=source_counts.index, autopct='%1.1f%%', colors=colors)
    axes[0, 2].set_title('Message Volume by Source')

    # 4) Overall sentiment label distribution
    label_counts = df['label'].value_counts()
    color_map = {'positive': '#2ecc71', 'neutral': '#95a5a6', 'negative': '#e74c3c', 'no_data': '#bdc3c7'}
    bar_colors = [color_map.get(label, '#3498db') for label in label_counts.index]
    bars = axes[1, 0].bar(label_counts.index, label_counts.values, color=bar_colors)
    axes[1, 0].set_title('Overall Sentiment Distribution')
    axes[1, 0].set_ylabel('Number of Days')
    for bar in bars:
        h = bar.get_height()
        axes[1, 0].text(bar.get_x() + bar.get_width()/2., h, f'{int(h)}', ha='center', va='bottom')

    # 5) Rolling average (skip no-data days)
    window = min(7, max(1, len(df) // 3))
    for source in df['source'].unique():
        sd = df[df['source'] == source].sort_values('date')
        real = sd[sd['count'] > 0]
        if len(real) >= window:
            rolling_avg = real['avg_score'].rolling(window=window, center=True).mean()
            axes[1, 1].plot(real['date'], rolling_avg, linewidth=3, label=f'{source} ({window}-day avg)', alpha=0.85)
    axes[1, 1].set_title(f'Rolling Average Sentiment ({window}-day window)')
    axes[1, 1].set_xlabel('Date')
    axes[1, 1].set_ylabel('Rolling Average Score')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)

    # 6) If multiple sources: correlation; else daily message volume
    if len(df['source'].unique()) > 1:
        pivot_df = df.pivot(index='date', columns='source', values='avg_score')
        im = axes[1, 2].imshow(pivot_df.corr(), cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
        axes[1, 2].set_xticks(range(len(pivot_df.columns)))
        axes[1, 2].set_yticks(range(len(pivot_df.columns)))
        axes[1, 2].set_xticklabels(pivot_df.columns, rotation=45)
        axes[1, 2].set_yticklabels(pivot_df.columns)
        axes[1, 2].set_title('Source Sentiment Correlation')
        plt.colorbar(im, ax=axes[1, 2], shrink=0.8)
    else:
        daily_counts = df.groupby('date')['count'].sum().sort_index()
        axes[1, 2].bar(daily_counts.index, daily_counts.values, alpha=0.7)
        axes[1, 2].set_title('Daily Message Volume')
        axes[1, 2].set_xlabel('Date')
        axes[1, 2].set_ylabel('Message Count')
        axes[1, 2].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.show()

def plot_sentiment_with_price(df: pd.DataFrame):
    """
    Overlay BTC price on top of daily sentiment (per source).
    Expects columns: date, avg_score, source, btc_close
    """
    if df.empty:
        print("No data for price overlay")
        return

    d = df.copy().sort_values("date")
    if not np.issubdtype(d["date"].dtype, np.datetime64):
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"])

    fig, ax1 = plt.subplots(figsize=(14, 7))
    # sentiment lines
    sources = d["source"].unique().tolist() if "source" in d.columns else ["telegram"]
    for s in sources:
        dd = d if s == "telegram" and "source" not in d.columns else d[d["source"] == s]
        ax1.plot(dd["date"], dd["avg_score"], alpha=0.35, linewidth=1.5, label=f"{s} daily")
        # 7d smoothing
        dd = dd.sort_values("date")
        if len(dd) >= 3:
            ax1.plot(dd["date"], dd["avg_score"].rolling(7, min_periods=3).mean(),
                     linewidth=3, label=f"{s} 7d avg")

    ax1.axhline(50, ls="--", c="gray", alpha=0.6)
    ax1.set_ylabel("Sentiment (0–100)")
    ax1.set_xlabel("Date")

    # price on twin axis
    ax2 = ax1.twinx()
    if d["btc_close"].notna().any():
        ax2.plot(d["date"], d["btc_close"], color="black", linewidth=2.0, label="BTC close (USDT)")
        ax2.set_ylabel("BTC Price (USDT)")

    # legends
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left", ncol=2, fontsize=9)

    ax1.set_title("Sentiment vs BTC Price (Binance spot, daily close)", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.25)
    plt.tight_layout()
    plt.show()