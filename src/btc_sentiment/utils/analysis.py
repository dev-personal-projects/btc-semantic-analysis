import pandas as pd
from typing import List, Dict

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

    real = df[df['count'] > 0]  # ignore synthetic no-data rows for score stats
    metrics = {
        'total_days': len(df),
        'total_messages': df['count'].sum(),
        'date_range': {'start': df['date'].min().strftime('%Y-%m-%d'),
                       'end': df['date'].max().strftime('%Y-%m-%d')},
        'overall_sentiment': {
            'mean_score': real['avg_score'].mean(),
            'median_score': real['avg_score'].median(),
            'std_score': real['avg_score'].std(),
            'min_score': real['avg_score'].min(),
            'max_score': real['avg_score'].max(),
        },
        'sentiment_distribution': df['label'].value_counts().to_dict(),
        'by_source': {}
    }

    for source in df['source'].unique():
        s = df[df['source'] == source]
        s_real = s[s['count'] > 0]
        metrics['by_source'][source] = {
            'days': len(s),
            'messages': int(s['count'].sum()),
            'avg_sentiment': s_real['avg_score'].mean(),
            'sentiment_std': s_real['avg_score'].std(),
            'label_distribution': s['label'].value_counts().to_dict()
        }
    return metrics

def generate_insights(df: pd.DataFrame) -> List[str]:
    """Generate actionable insights from sentiment data"""
    if df.empty:
        return ["No data available for analysis"]

    insights = []
    m = calculate_sentiment_metrics(df)
    overall = m['overall_sentiment']['mean_score']

    if overall > 60:
        insights.append(f"📈 Overall sentiment is positive (avg: {overall:.1f})")
    elif overall < 40:
        insights.append(f"📉 Overall sentiment is negative (avg: {overall:.1f})")
    else:
        insights.append(f"📊 Overall sentiment is neutral (avg: {overall:.1f})")

    vol = m['overall_sentiment']['std_score']
    if vol > 15:
        insights.append(f"⚡ High sentiment volatility detected (std: {vol:.1f})")
    elif vol < 5:
        insights.append(f"🔒 Sentiment is very stable (std: {vol:.1f})")

    total = m['total_messages']
    if total < 100:
        insights.append(f"⚠️ Limited data available ({total} messages)")
    elif total > 1000:
        insights.append(f"✅ Rich dataset with {total:,} messages analyzed")

    return insights
