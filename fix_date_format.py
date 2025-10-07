import os
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from datetime import datetime
import certifi
from dateutil import parser

load_dotenv()

MONGODB_URI = os.environ.get("MONGODB_URI")
DB_NAME = "development"

client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
ai_insight = db["ai_insight"]

print("=" * 60)
print("Fix Date Format Script")
print("=" * 60)

def parse_date(date_value):
    """Convert string date to datetime object."""
    if isinstance(date_value, datetime):
        return date_value
    if isinstance(date_value, str):
        return parser.parse(date_value)
    return None

def fix_date_formats():
    # Get all records
    print("📊 Fetching records...")
    records = list(ai_insight.find({}, {
        "reference_date": 1,
        "payment_date": 1,
        "created_at": 1
    }))
    
    print(f"✅ Found {len(records)} records\n")
    
    BATCH_SIZE = 1000
    total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    total_fixed = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        bulk_ops = []
        
        for record in batch:
            update_fields = {}
            
            # Fix reference_date
            ref_date = record.get("reference_date")
            if ref_date and isinstance(ref_date, str):
                update_fields["reference_date"] = parse_date(ref_date)
            
            # Fix payment_date
            pay_date = record.get("payment_date")
            if pay_date and isinstance(pay_date, str):
                update_fields["payment_date"] = parse_date(pay_date)
            
            # Fix created_at
            created = record.get("created_at")
            if created and isinstance(created, str):
                update_fields["created_at"] = parse_date(created)
            
            if update_fields:
                bulk_ops.append(
                    UpdateOne(
                        {"_id": record["_id"]},
                        {"$set": update_fields}
                    )
                )
        
        if bulk_ops:
            result = ai_insight.bulk_write(bulk_ops)
            total_fixed += result.modified_count
            batch_num = i//BATCH_SIZE + 1
            print(f"✅ Batch {batch_num}/{total_batches}: Fixed {result.modified_count} records")
    
    print(f"\n✅ Total fixed: {total_fixed}")
    print("🎉 All dates converted to DateTime format!")

if __name__ == "__main__":
    fix_date_formats()
    client.close()