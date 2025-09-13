
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from btc_sentiment.pipelines.price_sentiment_pipeline import run_price_sentiment_pipeline

if __name__ == "__main__":
    df = run_price_sentiment_pipeline(days_back=90, include_today=True)
    if df is not None and not df.empty:
        print("✅ Combined sentiment + price ready.")
