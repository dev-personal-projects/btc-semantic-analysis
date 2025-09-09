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
    df['date'] = pd.to_datetime(df['date'])

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Bitcoin Sentiment Analysis Dashboard', fontsize=16, fontweight='bold')

    # 1. Daily sentiment trend
    for source in df['source'].unique():
        source_data = df[df['source'] == source].sort_values('date')
        y = source_data["avg_score"].where(~((source_data["count"] == 0) | (source_data["label"] == "no_data")), np.nan)
        axes[0, 0].plot(source_data['date'], y, marker='o', linewidth=2, label=source, alpha=0.85)
    axes[0, 0].axhline(50, linestyle="--", linewidth=1, alpha=0.6, color="gray")
    axes[0, 0].set_title('Daily Sentiment Trend')
    axes[0, 0].set_xlabel('Date')
    axes[0, 0].set_ylabel('Average Sentiment Score')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # 2. Sentiment distribution by source
    sentiment_data, labels = [], []
    for source in df['source'].unique():
        sentiment_data.append(df[df['source'] == source & (df['count'] > 0)]['avg_score'])
        labels.append(source)
    axes[0, 1].boxplot(sentiment_data, labels=labels)
    axes[0, 1].set_title('Sentiment Score Distribution by Source')
    axes[0, 1].set_ylabel('Sentiment Score')

    # 3. Message volume by source
    source_counts = df.groupby('source')['count'].sum()
    colors = plt.cm.Set3(np.linspace(0, 1, len(source_counts)))
    axes[0, 2].pie(source_counts.values, labels=source_counts.index, autopct='%1.1f%%', colors=colors)
    axes[0, 2].set_title('Message Volume by Source')

    # 4. Sentiment label distribution
    label_counts = df['label'].value_counts()
    color_map = {'positive': '#2ecc71', 'neutral': '#95a5a6', 'negative': '#e74c3c', 'no_data': '#bdc3c7'}
    bar_colors = [color_map.get(label, '#3498db') for label in label_counts.index]
    bars = axes[1, 0].bar(label_counts.index, label_counts.values, color=bar_colors)
    axes[1, 0].set_title('Overall Sentiment Distribution')
    axes[1, 0].set_ylabel('Number of Days')
    for bar in bars:
        height = bar.get_height()
        axes[1, 0].text(bar.get_x() + bar.get_width()/2., height, f'{int(height)}', ha='center', va='bottom')

    # 5. Rolling average sentiment
    window = min(7, max(1, len(df) // 3))
    for source in df['source'].unique():
        source_data = df[df['source'] == source].sort_values('date')
        real = source_data[source_data['count'] > 0]
        if len(real) >= window:
            rolling_avg = real['avg_score'].rolling(window=window, center=True).mean()
            axes[1, 1].plot(real['date'], rolling_avg, linewidth=3, label=f'{source} ({window}-day avg)', alpha=0.85)
    axes[1, 1].set_title(f'Rolling Average Sentiment ({window}-day window)')
    axes[1, 1].set_xlabel('Date')
    axes[1, 1].set_ylabel('Rolling Average Score')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)

    # 6. If 1 source → show daily message counts; else correlation
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
