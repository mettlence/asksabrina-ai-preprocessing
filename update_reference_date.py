import os
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from datetime import datetime
import certifi

load_dotenv()

MONGODB_URI = os.environ.get("MONGODB_URI")
DB_NAME = "development"

if not MONGODB_URI:
    print("❌ Error: MONGODB_URI not found in .env file")
    exit(1)

print("=" * 60)
print("Reference Date Migration Script")
print("=" * 60)

# Fix SSL certificate issue
client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
orders_col = db["orders"]
ai_insight = db["ai_insight"]

def update_reference_dates():
    """Update existing ai_insight records with payment_date and reference_date."""
    
    start_time = datetime.now()
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get all ai_insight records (remove the exists check)
    print("📊 Fetching records to update...")
    insights = list(ai_insight.find(
        {},  # Get ALL records
        {"source_id": 1, "created_at": 1, "payment_status": 1, "reference_date": 1}
    ))
    
    # Filter only those without reference_date
    insights_to_update = [i for i in insights if "reference_date" not in i]
    
    print(f"✅ Total records: {len(insights)}")
    print(f"✅ Records to update: {len(insights_to_update)}\n")
    
    if not insights_to_update:
        print("✨ All records already have reference_date. Nothing to do!")
        return
    
    insights = insights_to_update
    
    # Get corresponding orders with payment_date
    print("📊 Fetching payment dates from orders...")
    from bson import ObjectId
    order_ids = [ObjectId(i['source_id']) for i in insights]
    orders = {
        str(o["_id"]): o.get("paymentDate")
        for o in orders_col.find(
            {"_id": {"$in": order_ids}},
            {"paymentDate": 1}
        )
    }
    print(f"✅ Fetched {len(orders)} order records\n")
    
    # Process in batches
    BATCH_SIZE = 1000
    total_batches = (len(insights) + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    paid_count = 0
    unpaid_count = 0
    
    print(f"🔄 Processing {len(insights)} records in {total_batches} batches...\n")
    
    for i in range(0, len(insights), BATCH_SIZE):
        batch = insights[i:i + BATCH_SIZE]
        bulk_ops = []
        batch_paid = 0
        batch_unpaid = 0
        
        for insight in batch:
            source_id = insight["source_id"]
            payment_status = insight.get("payment_status", 0)
            created_at = insight.get("created_at")
            payment_date = orders.get(source_id)
            
            # Determine reference_date
            reference_date = payment_date if (payment_status == 1 and payment_date) else created_at
            
            if payment_status == 1 and payment_date:
                batch_paid += 1
            else:
                batch_unpaid += 1
            
            bulk_ops.append(
                UpdateOne(
                    {"_id": insight["_id"]},
                    {
                        "$set": {
                            "payment_date": payment_date,
                            "reference_date": reference_date,
                            "pipeline_version": "v2.2"
                        }
                    }
                )
            )
        
        if bulk_ops:
            result = ai_insight.bulk_write(bulk_ops)
            total_updated += result.modified_count
            paid_count += batch_paid
            unpaid_count += batch_unpaid
            
            batch_num = i//BATCH_SIZE + 1
            print(f"✅ Batch {batch_num}/{total_batches}: Updated {result.modified_count} records")
            print(f"   └─ Paid: {batch_paid}, Unpaid: {batch_unpaid}")
    
    print("\n" + "=" * 60)
    print("📊 Summary")
    print("=" * 60)
    print(f"✅ Total updated: {total_updated}/{len(insights)}")
    print(f"💰 Paid orders: {paid_count}")
    print(f"⏳ Unpaid orders: {unpaid_count}")
    
    # Create indexes
    print("\n🔧 Creating indexes...")
    ai_insight.create_index("reference_date")
    ai_insight.create_index("payment_date")
    print("✅ Indexes created: reference_date, payment_date")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"\n⏱️  Duration: {duration:.2f} seconds")
    print(f"🎉 Migration completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    from bson import ObjectId
    update_reference_dates()
    client.close()