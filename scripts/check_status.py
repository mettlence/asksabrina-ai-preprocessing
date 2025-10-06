#!/usr/bin/env python3
"""Check the status of AI insights preprocessing."""

import os
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def main():
    client = MongoClient(os.getenv("MONGODB_URI"))
    db = client["asksabrina"]
    
    # Total processed
    total = db.ai_insight.count_documents({})
    print(f"📊 Total processed orders: {total}")
    
    # Last 24 hours
    day_ago = datetime.utcnow() - timedelta(days=1)
    last_24h = db.ai_insight.count_documents({
        "processed_at": {"$gte": day_ago}
    })
    print(f"📈 Processed in last 24h: {last_24h}")
    
    # Last 6 hours
    six_hours_ago = datetime.utcnow() - timedelta(hours=6)
    last_6h = db.ai_insight.count_documents({
        "processed_at": {"$gte": six_hours_ago}
    })
    print(f"⏰ Processed in last 6h: {last_6h}")
    
    # Latest processing
    latest = db.ai_insight.find_one(
        {},
        sort=[("processed_at", -1)]
    )
    
    if latest:
        latest_time = latest.get("processed_at")
        time_diff = datetime.utcnow() - latest_time
        hours_ago = time_diff.total_seconds() / 3600
        print(f"🕐 Last processed: {hours_ago:.1f} hours ago")
        print(f"📝 Pipeline version: {latest.get('pipeline_version', 'unknown')}")
    
    # Unprocessed orders
    lookback = datetime.utcnow() - timedelta(hours=720)
    processed_ids = db.ai_insight.distinct("source_id")
    
    unprocessed = db.orders.count_documents({
        "createdAt": {"$gte": lookback},
        "_id": {"$nin": [eval(f"ObjectId('{x}')") for x in processed_ids if x]}
    })
    print(f"⏳ Pending orders: {unprocessed}")
    
    client.close()

if __name__ == "__main__":
    main()