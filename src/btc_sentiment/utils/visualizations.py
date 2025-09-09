import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import List
from ..services.aggregator import DailySentiment

# Set style for better visualizations
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def plot_daily_sentiment(records: List[DailySentiment]):
    """Simple daily sentiment plot"""
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

def create_sentiment_dashboard(records: List[DailySentiment]):
    """Create a comprehensive sentiment analysis dashboard"""
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
        axes[0,0].plot(source_data['date'], source_data['avg_score'], 
                      marker='o', linewidth=2, label=source, alpha=0.8)
    axes[0,0].set_title('Daily Sentiment Trend')
    axes[0,0].set_xlabel('Date')
    axes[0,0].set_ylabel('Average Sentiment Score')
    axes[0,0].legend()
    axes[0,0].grid(True, alpha=0.3)
    
    # 2. Sentiment distribution by source
    sentiment_data = []
    labels = []
    for source in df['source'].unique():
        source_data = df[df['source'] == source]['avg_score']
        sentiment_data.append(source_data)
        labels.append(source)
    
    axes[0,1].boxplot(sentiment_data, labels=labels)
    axes[0,1].set_title('Sentiment Score Distribution by Source')
    axes[0,1].set_ylabel('Sentiment Score')
    
    # 3. Message volume by source
    source_counts = df.groupby('source')['count'].sum()
    colors = plt.cm.Set3(np.linspace(0, 1, len(source_counts)))
    wedges, texts, autotexts = axes[0,2].pie(source_counts.values, labels=source_counts.index, 
                                            autopct='%1.1f%%', colors=colors)
    axes[0,2].set_title('Message Volume by Source')
    
    # 4. Sentiment label distribution
    label_counts = df['label'].value_counts()
    color_map = {'positive': '#2ecc71', 'neutral': '#95a5a6', 'negative': '#e74c3c'}
    bar_colors = [color_map.get(label, '#3498db') for label in label_counts.index]
    bars = axes[1,0].bar(label_counts.index, label_counts.values, color=bar_colors)
    axes[1,0].set_title('Overall Sentiment Distribution')
    axes[1,0].set_ylabel('Number of Days')
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        axes[1,0].text(bar.get_x() + bar.get_width()/2., height,
                      f'{int(height)}', ha='center', va='bottom')
    
    # 5. Rolling average sentiment
    window = min(7, len(df) // 3)  # 7-day or adaptive window
    for source in df['source'].unique():
        source_data = df[df['source'] == source].sort_values('date')
        if len(source_data) >= window:
            rolling_avg = source_data['avg_score'].rolling(window=window, center=True).mean()
            axes[1,1].plot(source_data['date'], rolling_avg, 
                          linewidth=3, label=f'{source} ({window}-day avg)', alpha=0.8)
    
    axes[1,1].set_title(f'Rolling Average Sentiment ({window}-day window)')
    axes[1,1].set_xlabel('Date')
    axes[1,1].set_ylabel('Rolling Average Score')
    axes[1,1].legend()
    axes[1,1].grid(True, alpha=0.3)
    
    # 6. Correlation heatmap (if multiple sources)
    if len(df['source'].unique()) > 1:
        pivot_df = df.pivot(index='date', columns='source', values='avg_score')
        correlation_matrix = pivot_df.corr()
        
        im = axes[1,2].imshow(correlation_matrix, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
        axes[1,2].set_xticks(range(len(correlation_matrix.columns)))
        axes[1,2].set_yticks(range(len(correlation_matrix.columns)))
        axes[1,2].set_xticklabels(correlation_matrix.columns, rotation=45)
        axes[1,2].set_yticklabels(correlation_matrix.columns)
        axes[1,2].set_title('Source Sentiment Correlation')
        
        # Add correlation values
        for i in range(len(correlation_matrix.columns)):
            for j in range(len(correlation_matrix.columns)):
                text = axes[1,2].text(j, i, f'{correlation_matrix.iloc[i, j]:.2f}',
                                    ha="center", va="center", color="black", fontweight='bold')
        
        plt.colorbar(im, ax=axes[1,2], shrink=0.8)
    else:
        # Show daily message counts if only one source
        daily_counts = df.groupby('date')['count'].sum().sort_index()
        axes[1,2].bar(daily_counts.index, daily_counts.values, alpha=0.7)
        axes[1,2].set_title('Daily Message Volume')
        axes[1,2].set_xlabel('Date')
        axes[1,2].set_ylabel('Message Count')
        axes[1,2].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.show()

def plot_sentiment_summary(records: List[DailySentiment]):
    """Create a summary statistics table and basic metrics"""
    if not records:
        print("No data available")
        return
    
    df = pd.DataFrame([r.dict() for r in records])
    df['date'] = pd.to_datetime(df['date'])
    
    print("=== SENTIMENT ANALYSIS SUMMARY ===")
    print(f"Analysis Period: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    print(f"Total Days Analyzed: {len(df)}")
    print(f"Total Messages: {df['count'].sum():,}")
    print()
    
    # Summary by source
    print("BY SOURCE:")
    summary = df.groupby('source').agg({
        'avg_score': ['mean', 'std', 'min', 'max'],
        'count': 'sum',
        'date': 'count'
    }).round(2)
    
    summary.columns = ['Avg_Score', 'Std_Score', 'Min_Score', 'Max_Score', 'Total_Messages', 'Days']
    print(summary)
    print()
    
    # Overall sentiment distribution
    print("SENTIMENT DISTRIBUTION:")
    sentiment_dist = df['label'].value_counts()
    for label, count in sentiment_dist.items():
        percentage = (count / len(df)) * 100
        print(f"{label.capitalize()}: {count} days ({percentage:.1f}%)")

def plot_time_series_analysis(records: List[DailySentiment]):
    """Detailed time series analysis with trends and patterns"""
    if not records:
        print("No data available for time series analysis")
        return
    
    df = pd.DataFrame([r.dict() for r in records])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    fig, axes = plt.subplots(3, 1, figsize=(15, 12))
    fig.suptitle('Time Series Analysis', fontsize=16, fontweight='bold')
    
    # 1. Raw sentiment scores with trend
    for source in df['source'].unique():
        source_data = df[df['source'] == source]
        axes[0].scatter(source_data['date'], source_data['avg_score'], 
                       alpha=0.6, label=f'{source} (raw)', s=50)
        
        # Add trend line
        if len(source_data) > 1:
            z = np.polyfit(range(len(source_data)), source_data['avg_score'], 1)
            p = np.poly1d(z)
            axes[0].plot(source_data['date'], p(range(len(source_data))), 
                        "--", alpha=0.8, linewidth=2, label=f'{source} (trend)')
    
    axes[0].set_title('Sentiment Scores with Trend Lines')
    axes[0].set_ylabel('Sentiment Score')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    
    # 2. Message volume over time
    daily_volume = df.groupby('date')['count'].sum()
    axes[1].fill_between(daily_volume.index, daily_volume.values, alpha=0.7, color='skyblue')
    axes[1].plot(daily_volume.index, daily_volume.values, color='navy', linewidth=2)
    axes[1].set_title('Daily Message Volume')
    axes[1].set_ylabel('Message Count')
    axes[1].grid(True, alpha=0.3)
    
    # 3. Sentiment volatility (rolling standard deviation)
    window = min(7, len(df) // 3)
    for source in df['source'].unique():
        source_data = df[df['source'] == source].sort_values('date')
        if len(source_data) >= window:
            rolling_std = source_data['avg_score'].rolling(window=window).std()
            axes[2].plot(source_data['date'], rolling_std, 
                        marker='o', label=f'{source} volatility', linewidth=2)
    
    axes[2].set_title(f'Sentiment Volatility ({window}-day rolling std)')
    axes[2].set_xlabel('Date')
    axes[2].set_ylabel('Standard Deviation')
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()