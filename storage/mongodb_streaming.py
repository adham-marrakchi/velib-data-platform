import json
import logging
import os
import sys
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "iot_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sensor_readings")
MAX_RETRIES = 5
RETRY_BACKOFF = 5


def consume_kafka_to_mongodb():
    """Consume Kafka messages and store in MongoDB."""
    from pymongo import MongoClient

    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    try:
        mongo_client.admin.command("ping")
        logger.info("Connected to MongoDB at %s", MONGODB_URI)
    except Exception as e:
        logger.error("Failed to connect to MongoDB: %s", e)
        sys.exit(1)

    consumer = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="mongodb_consumer",
            )
            logger.info("Connected to Kafka, listening on topic: %s", KAFKA_TOPIC)
            break
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d). Retrying...",
                attempt,
                MAX_RETRIES,
            )
            time.sleep(RETRY_BACKOFF)

    if consumer is None:
        logger.error("Failed to connect to Kafka.")
        sys.exit(1)

    messages_stored = 0
    for message in consumer:
        data = message.value
        device_id = data.get("device_id")
        reading_value = data.get("reading_value")

        if device_id is None or reading_value is None:
            logger.warning("Skipping malformed message: %s", data)
            continue

        collection.insert_one(
            {
                "device_id": device_id,
                "reading_value": reading_value,
                "timestamp": data.get("timestamp", int(datetime.utcnow().timestamp())),
                "ingested_at": datetime.utcnow(),
            }
        )
        messages_stored += 1

        if messages_stored % 100 == 0:
            logger.info("Stored %d messages in MongoDB.", messages_stored)


if __name__ == "__main__":
    consume_kafka_to_mongodb()
