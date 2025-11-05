import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from openai import OpenAI
from dotenv import load_dotenv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import hashlib
from dateutil import parser
import requests

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('preprocess.log')
    ]
)
logger = logging.getLogger(__name__)

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

MONGODB_SOURCE_URI = os.environ.get("MONGODB_SOURCE_URI")  # Atlas
MONGODB_TARGET_URI = os.environ.get("MONGODB_TARGET_URI")  # EC2
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

if not MONGODB_SOURCE_URI or not MONGODB_TARGET_URI or not OPENAI_API_KEY:
    logger.error("Missing required environment variables (MONGODB_SOURCE_URI, MONGODB_TARGET_URI, OPENAI_API_KEY)")
    sys.exit(1)

DB_NAME = "development"
ORDERS_COLLECTION = "orders"
CUSTOMERS_COLLECTION = "customers"
TARGET_DB_NAME = "asksabrina"
TARGET_COLLECTION = "ai_insight"

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "720"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
PROCESS_PAID_ONLY = os.getenv("PROCESS_PAID_ONLY", "false").lower() == "true"
SKIP_UNCHANGED = os.getenv("SKIP_UNCHANGED", "true").lower() == "true"

# -----------------------------
# MongoDB setup - Source (Atlas) and Target (EC2)
# -----------------------------

# SOURCE: Atlas MongoDB (read orders & customers)
logger.info("Connecting to SOURCE MongoDB (Atlas)...")
source_client = MongoClient(
    MONGODB_SOURCE_URI,
    maxPoolSize=50,
    minPoolSize=10,
    maxIdleTimeMS=45000,
    connectTimeoutMS=10000,
    serverSelectionTimeoutMS=10000
)
source_db = source_client[DB_NAME]
orders_col = source_db[ORDERS_COLLECTION]
customers_col = source_db[CUSTOMERS_COLLECTION]

# TARGET: EC2 MongoDB (write ai_insight)
logger.info("Connecting to TARGET MongoDB (EC2)...")
target_client = MongoClient(
    MONGODB_TARGET_URI,
    maxPoolSize=20,
    minPoolSize=5,
    connectTimeoutMS=10000,
    serverSelectionTimeoutMS=10000
)
target_db = target_client[TARGET_DB_NAME]
ai_insight = target_db[TARGET_COLLECTION]

# Create indexes on TARGET only
try:
    ai_insight.create_index("source_id", unique=True)
    ai_insight.create_index("processed_at")
    ai_insight.create_index("customer_id")
    ai_insight.create_index("content_hash")
    ai_insight.create_index("reference_date")
    ai_insight.create_index("payment_date")
    logger.info("Target database indexes created/verified")
except Exception as e:
    logger.warning(f"Index creation warning: {e}")

# -----------------------------
# OpenAI client
# -----------------------------
openai = OpenAI(api_key=OPENAI_API_KEY)


# -----------------------------
# Slack notification
# -----------------------------
def send_slack_notification(message: str, is_error: bool = False):
    """Send notification to Slack."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return
    
    try:
        color = "#FF0000" if is_error else "#36a64f"
        payload = {
            "attachments": [{
                "color": color,
                "text": message,
                "footer": "AI Preprocessing Pipeline",
                "ts": int(datetime.utcnow().timestamp())
            }]
        }
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
        if response.status_code == 200:
            logger.info("Slack notification sent")
        else:
            logger.warning(f"Slack notification failed: {response.status_code}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


# -----------------------------
# Date parsing helper
# -----------------------------
def parse_date(date_value) -> Optional[datetime]:
    """Convert string date to datetime object, handle various formats."""
    if isinstance(date_value, datetime):
        return date_value
    if isinstance(date_value, str):
        try:
            return parser.parse(date_value)
        except Exception as e:
            logger.warning(f"Failed to parse date: {date_value}, error: {e}")
            return None
    return None


# -----------------------------
# Generate content hash
# -----------------------------
def generate_content_hash(order: Dict) -> str:
    """Generate hash of order content to detect changes."""
    content = {
        "questions": order.get("question", []),
        "tarot_cards": order.get("tarotCards", []),
        "payment_status": order.get("paymentStatus", 0)
    }
    content_str = json.dumps(content, sort_keys=True, default=str)
    return hashlib.md5(content_str.encode()).hexdigest()


# -----------------------------
# Get unprocessed orders
# -----------------------------
def get_unprocessed_orders() -> List[Dict]:
    """Fetch orders from SOURCE that need processing."""
    try:
        since = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
        
        # Smart match criteria for paid/unpaid orders
        match_criteria = {
            "$or": [
                # Paid orders with recent payment date
                {
                    "paymentStatus": 1,
                    "paymentDate": {"$exists": True, "$gte": since}
                },
                # Paid orders recently created
                {
                    "paymentStatus": 1,
                    "createdAt": {"$gte": since}
                },
                # Unpaid orders recently created
                {
                    "paymentStatus": {"$ne": 1},
                    "createdAt": {"$gte": since}
                }
            ]
        }
        
        # Optional: Only process paid orders
        if PROCESS_PAID_ONLY:
            match_criteria = {
                "paymentStatus": 1,
                "$or": [
                    {"paymentDate": {"$exists": True, "$gte": since}},
                    {"createdAt": {"$gte": since}}
                ]
            }
            logger.info("Processing PAID ORDERS ONLY (paymentStatus=1)")
        
        # Get processed orders from TARGET
        processed_ids = set()
        processed_data = {}
        try:
            processed_docs = ai_insight.find({}, {"source_id": 1, "content_hash": 1, "payment_status": 1})
            for doc in processed_docs:
                source_id = doc.get("source_id")
                processed_ids.add(source_id)
                processed_data[source_id] = {
                    "content_hash": doc.get("content_hash"),
                    "payment_status": doc.get("payment_status")
                }
            logger.info(f"Found {len(processed_ids)} already processed orders in TARGET")
        except Exception as e:
            logger.warning(f"Error fetching processed orders from TARGET: {e}")
        
        # Fetch orders from SOURCE
        orders = list(orders_col.find(match_criteria))
        logger.info(f"Found {len(orders)} orders from SOURCE matching criteria")
        
        # Filter based on SKIP_UNCHANGED setting
        if SKIP_UNCHANGED:
            new_or_changed = []
            skipped = 0
            
            for order in orders:
                order_id = str(order["_id"])
                
                if order_id not in processed_ids:
                    new_or_changed.append(order)
                else:
                    current_hash = generate_content_hash(order)
                    existing_data = processed_data.get(order_id, {})
                    existing_hash = existing_data.get("content_hash")
                    
                    current_payment_status = order.get("paymentStatus", 0)
                    existing_payment_status = existing_data.get("payment_status", 0)
                    
                    if current_hash != existing_hash or current_payment_status != existing_payment_status:
                        new_or_changed.append(order)
                        reason = "content changed" if current_hash != existing_hash else "payment status changed"
                        logger.info(f"Order {order['_id']} {reason}, will reprocess")
                    else:
                        skipped += 1
            
            logger.info(f"Found {len(new_or_changed)} orders to process ({skipped} unchanged, skipped)")
            return new_or_changed
        else:
            unprocessed = [o for o in orders if str(o["_id"]) not in processed_ids]
            logger.info(f"Found {len(unprocessed)} unprocessed orders")
            return unprocessed
            
    except Exception as e:
        logger.error(f"Error fetching unprocessed orders: {e}")
        return []


# -----------------------------
# Batch fetch customers
# -----------------------------
def batch_fetch_customers(customer_ids: List[ObjectId]) -> Dict[str, Dict]:
    """Fetch multiple customers from SOURCE in one query."""
    try:
        customers = customers_col.find({"_id": {"$in": customer_ids}})
        return {str(c["_id"]): c for c in customers}
    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        return {}


# -----------------------------
# Extract customer info
# -----------------------------
def extract_customer_info(customer: Optional[Dict]) -> Dict[str, Any]:
    """Extract relevant customer information."""
    if not customer:
        return {}
    
    return {
        "fullName": customer.get("fullName"),
        "firstName": customer.get("firstName"),
        "lastName": customer.get("lastName"),
        "email": customer.get("email"),
        "gender": customer.get("gender"),
        "age": customer.get("age"),
        "horoscope": customer.get("horoscope"),
        "birthday": customer.get("birthday"),
        "martialStatus": customer.get("martialStatus"),
        "country": customer.get("country"),
        "city": customer.get("city"),
    }


# -----------------------------
# Generate embedding
# -----------------------------
def generate_embedding(text: str) -> Optional[List[float]]:
    """Generate embedding for text."""
    try:
        response = openai.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return response.data[0].embedding
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        return None


# -----------------------------
# Generate AI insights
# -----------------------------
def generate_insights(raw_text: str, customer_info: Dict) -> Dict:
    """Generate AI insights using OpenAI."""
    insight_prompt = f"""Analyze the following tarot-related customer question and return structured marketing and product insights.

Customer details:
{json.dumps(customer_info, indent=2, default=str)}

Questions:
{raw_text}

Respond in JSON with the following fields:
- keywords: list of 3-7 key terms from the question
- topics: list of 1-3 topics with {{name, confidence}} where confidence is 0-1
- sentiment: {{label: "positive"|"neutral"|"negative", score: 0-1}}
- emotional_tone: list of 1-3 emotions with {{emotion, score}} where score is 0-1
- insight_tags: list of 2-5 tags for marketing or segmentation
- possible_needs: list of 1-3 inferred customer needs or desires"""

    try:
        completion = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise NLP analyst for customer insights. Always respond with valid JSON."},
                {"role": "user", "content": insight_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.3,
            max_tokens=800
        )
        
        raw_content = completion.choices[0].message.content
        insights = json.loads(raw_content)
        return insights
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error generating insights: {e}")
        return {}


# -----------------------------
# Process single order
# -----------------------------
def process_order(order: Dict, customer_cache: Dict[str, Dict]) -> Optional[Dict]:
    """Process a single order and return enriched document."""
    order_id = order.get("_id")
    customer_id = order.get("customerId")
    
    try:
        # Get customer from cache
        customer = customer_cache.get(str(customer_id)) if customer_id else None
        customer_info = extract_customer_info(customer)

        # Prepare question text
        questions = order.get("question", [])
        raw_text = " ".join(questions).strip()

        if not raw_text:
            logger.warning(f"Skipping order {order_id} — no question content")
            return None

        # Generate content hash
        content_hash = generate_content_hash(order)

        # Generate embedding
        embedding = generate_embedding(raw_text)
        if not embedding:
            logger.error(f"Failed to generate embedding for order {order_id}")
            return None

        # Generate AI insights
        insights = generate_insights(raw_text, customer_info)

        # Handle dates - ensure all are datetime objects
        payment_status = order.get("paymentStatus", 0)
        created_at = parse_date(order.get("createdAt"))
        payment_date_raw = order.get("paymentDate")
        payment_date = parse_date(payment_date_raw) if payment_date_raw else None
        
        # Determine reference_date (smart date for analytics)
        if payment_status == 1 and payment_date:
            reference_date = payment_date
        else:
            reference_date = created_at

        # Build enriched record
        enriched_doc = {
            "source_id": str(order_id),
            "customer_id": str(customer_id) if customer_id else None,
            "order_id": order.get("orderId"),
            "created_at": created_at,
            "payment_date": payment_date,
            "reference_date": reference_date,
            "payment_status": payment_status,
            "product_id": str(order.get("productId")) if order.get("productId") else None,
            "total_price": order.get("totalPrice"),
            "questions": questions,
            "raw_text": raw_text,
            "tarot_cards": order.get("tarotCards", []),
            "customer_info": customer_info,
            "embedding": embedding,
            "keywords": insights.get("keywords", []),
            "topics": insights.get("topics", []),
            "sentiment": insights.get("sentiment", {}),
            "emotional_tone": insights.get("emotional_tone", []),
            "insight_tags": insights.get("insight_tags", []),
            "possible_needs": insights.get("possible_needs", []),
            "content_hash": content_hash,
            "processed_at": datetime.utcnow(),
            "embedding_model": "text-embedding-3-small",
            "pipeline_version": "v2.2",
        }

        logger.info(f"Successfully processed order {order_id}")
        return enriched_doc

    except Exception as e:
        logger.error(f"Error processing order {order_id}: {e}")
        return None


# -----------------------------
# Batch insert to MongoDB
# -----------------------------
def batch_insert_insights(enriched_docs: List[Dict]) -> int:
    """Insert multiple documents to TARGET using bulk operations."""
    if not enriched_docs:
        return 0
    
    try:
        operations = [
            UpdateOne(
                {"source_id": doc["source_id"]},
                {"$set": doc},
                upsert=True
            )
            for doc in enriched_docs
        ]
        
        result = ai_insight.bulk_write(operations, ordered=False)
        inserted = result.upserted_count + result.modified_count
        logger.info(f"Bulk inserted/updated {inserted} documents to TARGET")
        return inserted
    except BulkWriteError as e:
        logger.error(f"Bulk write error: {e.details}")
        return 0
    except Exception as e:
        logger.error(f"Error in batch insert: {e}")
        return 0


# -----------------------------
# Process orders in parallel
# -----------------------------
def process_orders_parallel(orders: List[Dict], customer_cache: Dict[str, Dict]) -> List[Dict]:
    """Process multiple orders in parallel."""
    enriched_docs = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_order = {
            executor.submit(process_order, order, customer_cache): order 
            for order in orders
        }
        
        for future in as_completed(future_to_order):
            try:
                result = future.result()
                if result:
                    enriched_docs.append(result)
            except Exception as e:
                order = future_to_order[future]
                logger.error(f"Error processing order {order.get('_id')}: {e}")
    
    return enriched_docs


# -----------------------------
# Main
# -----------------------------
def main():
    """Main execution function."""
    start_time = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("Starting AI preprocessing pipeline (Atlas → EC2)...")
    logger.info(f"Lookback period: {LOOKBACK_HOURS} hours")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Process paid only: {PROCESS_PAID_ONLY}")
    logger.info(f"Skip unchanged orders: {SKIP_UNCHANGED}")
    
    try:
        # Fetch unprocessed orders from SOURCE
        orders = get_unprocessed_orders()
        
        if not orders:
            logger.info("No new orders to process")
            send_slack_notification("ℹ️ *No new orders to process*")
            return

        logger.info(f"Processing {len(orders)} orders...")

        # Batch fetch all customers from SOURCE
        customer_ids = [o.get("customerId") for o in orders if o.get("customerId")]
        customer_cache = batch_fetch_customers(customer_ids)
        logger.info(f"Fetched {len(customer_cache)} customer records from SOURCE")

        # Process in batches
        total_processed = 0
        for i in range(0, len(orders), BATCH_SIZE):
            batch = orders[i:i + BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (len(orders)-1)//BATCH_SIZE + 1
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} orders)")
            
            enriched_docs = process_orders_parallel(batch, customer_cache)
            inserted = batch_insert_insights(enriched_docs)
            total_processed += inserted

        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Pipeline completed successfully!")
        logger.info(f"Total processed: {total_processed}/{len(orders)}")
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        
        if len(orders) > 0:
            logger.info(f"Average: {duration/len(orders):.2f} seconds per order")
        
        logger.info("=" * 60)
        
        # Send success notification to Slack
        send_slack_notification(
            f"✅ *Preprocessing Completed*\n"
            f"• Processed: {total_processed}/{len(orders)} orders\n"
            f"• Duration: {duration/60:.1f} minutes\n"
            f"• Avg: {duration/len(orders):.1f}s per order"
        )

    except Exception as e:
        logger.error(f"Fatal error in main pipeline: {e}", exc_info=True)
        
        # Send error notification to Slack
        send_slack_notification(
            f"❌ *Preprocessing Failed*\n"
            f"• Error: {str(e)[:200]}\n"
            f"• Check logs for details",
            is_error=True
        )
        
        sys.exit(1)
    finally:
        source_client.close()
        target_client.close()
        logger.info("MongoDB connections closed")


if __name__ == "__main__":
    main()