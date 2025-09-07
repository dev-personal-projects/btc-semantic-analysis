import pandas as pd
import numpy as np
from typing import List, Dict
from ..services.aggregator import DailySentiment

def load_and_prepare_data(path: str = None) -> pd.DataFrame:
    """Load sentiment data and prepare it for analysis"""
    from .io import load_daily_sentiment
    
    records = load_daily_sentiment(path)
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame([r.dict() for r in records])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    return df

def calculate_sentiment_metrics(df: pd.DataFrame) -> Dict:
    """Calculate comprehensive sentiment metrics"""
    if df.empty:
        return {}
    
    metrics = {
        'total_days': len(df),
        'total_messages': df['count'].sum(),
        'date_range': {
            'start': df['date'].min().strftime('%Y-%m-%d'),
            'end': df['date'].max().strftime('%Y-%m-%d')
        },
        'overall_sentiment': {
            'mean_score': df['avg_score'].mean(),
            'median_score': df['avg_score'].median(),
            'std_score': df['avg_score'].std(),
            'min_score': df['avg_score'].min(),
            'max_score': df['avg_score'].max()
        },
        'sentiment_distribution': df['label'].value_counts().to_dict(),
        'by_source': {}
    }
    
    # Calculate metrics by source
    for source in df['source'].unique():
        source_data = df[df['source'] == source]
        metrics['by_source'][source] = {
            'days': len(source_data),
            'messages': source_data['count'].sum(),
            'avg_sentiment': source_data['avg_score'].mean(),
            'sentiment_std': source_data['avg_score'].std(),
            'label_distribution': source_data['label'].value_counts().to_dict()
        }
    
    return metrics

def generate_insights(df: pd.DataFrame) -> List[str]:
    """Generate actionable insights from sentiment data"""
    if df.empty:
        return ["No data available for analysis"]
    
    insights = []
    metrics = calculate_sentiment_metrics(df)
    
    # Overall sentiment insights
    overall_sentiment = metrics['overall_sentiment']['mean_score']
    if overall_sentiment > 60:
        insights.append(f"📈 Overall sentiment is positive (avg: {overall_sentiment:.1f})")
    elif overall_sentiment < 40:
        insights.append(f"📉 Overall sentiment is negative (avg: {overall_sentiment:.1f})")
    else:
        insights.append(f"📊 Overall sentiment is neutral (avg: {overall_sentiment:.1f})")
    
    # Volatility insights
    volatility = metrics['overall_sentiment']['std_score']
    if volatility > 15:
        insights.append(f"⚡ High sentiment volatility detected (std: {volatility:.1f})")
    elif volatility < 5:
        insights.append(f"🔒 Sentiment is very stable (std: {volatility:.1f})")
    
    # Data quality insights
    total_messages = metrics['total_messages']
    if total_messages < 100:
        insights.append(f"⚠️ Limited data available ({total_messages} messages)")
    elif total_messages > 1000:
        insights.append(f"✅ Rich dataset with {total_messages:,} messages analyzed")
    
    return insights