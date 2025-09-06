import pandas as pd
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from ..services.aggregator import DailySentiment

def save_records(records: List[DailySentiment], path: str):
    df = pd.DataFrame([r.dict() for r in records])
    p = Path(path)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_name = f"{p.stem}_{timestamp}{p.suffix}"
    timestamped_path = p.parent / timestamped_name
    
    timestamped_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(timestamped_path, index=False)
    df.to_parquet(p, index=False)
    
    print(f"Saved {len(records)} records to {timestamped_path}")
    return str(timestamped_path)

def load_daily_sentiment(path: Optional[str] = None) -> List[DailySentiment]:
    if path is None:
        path = "data/processed/daily_sentiment.parquet"
    
    try:
        df = pd.read_parquet(path)
        return [DailySentiment(**row) for _, row in df.iterrows()]
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"Error loading data from {path}: {e}")
        return []
