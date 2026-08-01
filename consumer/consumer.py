# consumer/consumer.py
import asyncio
import json
import os
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "colabnote_events")
KAFKA_GROUP_ID = "colabnote_activity_consumer"

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("MONGO_DB", "colabnote")


async def main():
    print("🔄 Starting Kafka consumer...")
    print(f"📡 Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📦 Topic: {KAFKA_TOPIC}")
    
    # Connect to MongoDB
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    db = mongo_client[DATABASE_NAME]
    
    # Ensure index exists
    await db.activity_logs.create_index([("user_id", 1), ("timestamp", -1)])
    
    # Create Kafka consumer
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    
    await consumer.start()
    print("✅ Connected. Listening for events...")
    
    try:
        async for message in consumer:
            try:
                event = json.loads(message.value.decode("utf-8"))
                
                # Write to MongoDB
                activity_doc = {
                    "event_type": event["event_type"],
                    "user_id": event["user_id"],
                    "resource_id": event.get("resource_id"),
                    "timestamp": event["timestamp"],
                    "metadata": event.get("metadata", {}),
                    "processed_at": datetime.utcnow(),
                }
                
                await db.activity_logs.insert_one(activity_doc)
                
                print(
                    f"✅ {event['event_type']} | "
                    f"user={event['user_id']} | "
                    f"resource={event.get('resource_id')}"
                )
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error: {e}")
            except Exception as e:
                print(f"❌ Error: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())