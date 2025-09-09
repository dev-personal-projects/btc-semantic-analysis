#!/usr/bin/env python3
"""Quick test to verify the datetime fix"""

import sys
sys.path.append('src')

from datetime import datetime, timezone, timedelta
from btc_sentiment.pipelines.ingest_pipeline import run_simple_analysis

print("🧪 Testing datetime fix...")
print("=" * 50)

# Test with a smaller time window first
DAYS_BACK = 7  # Reduced from 100 to test faster

try:
    print(f'🎯 Running sentiment analysis for last {DAYS_BACK} days...')
    daily_records = run_simple_analysis(days_back=DAYS_BACK)
    
    if daily_records:
        print(f"✅ SUCCESS! Generated {len(daily_records)} daily records")
    else:
        print("⚠️ No data returned, but no datetime error occurred")
        
except Exception as e:
    print(f"❌ Error: {e}")
    if "can't compare offset-naive and offset-aware datetimes" in str(e):
        print("💡 The datetime fix didn't work completely")
    else:
        print("💡 Different error - may need additional investigation")

print("\n🔍 Testing timezone handling directly...")
now_utc = datetime.now(timezone.utc)
now_naive = datetime.utcnow()
print(f"UTC aware: {now_utc}")
print(f"UTC naive: {now_naive}")

# Test comparison
try:
    result = now_utc > now_naive
    print("❌ This should have failed but didn't - there may be other issues")
except TypeError as e:
    print(f"✅ Expected error caught: {e}")
    print("✅ Our fix should handle this properly")