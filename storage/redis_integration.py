import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "processed_readings")


def process_streaming_data():
    """Consume Kafka sensor readings, cache in Redis, and forward processed data."""
    import redis

    from kafka import KafkaConsumer, KafkaProducer

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

    try:
        redis_client.ping()
        logger.info("Connected to Redis at %s:%d", REDIS_HOST, REDIS_PORT)
    except redis.ConnectionError:
        logger.error("Failed to connect to Redis.")
        sys.exit(1)

    from kafka.errors import NoBrokersAvailable

    try:
        consumer = KafkaConsumer(
            KAFKA_INPUT_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="redis_processor",
        )
    except NoBrokersAvailable:
        logger.error("Kafka broker not available at %s", KAFKA_BROKER)
        sys.exit(1)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
        )
    except NoBrokersAvailable:
        logger.error("Kafka producer cannot connect to %s", KAFKA_BROKER)
        sys.exit(1)

    logger.info(
        "Processing data from '%s' -> Redis -> '%s'",
        KAFKA_INPUT_TOPIC,
        KAFKA_OUTPUT_TOPIC,
    )
    messages_processed = 0

    for message in consumer:
        data = message.value
        device_id = data.get("device_id")
        reading_value = data.get("reading_value")

        if device_id is None or reading_value is None:
            continue

        # Cache latest reading in Redis
        cache_key = f"device:{device_id}:latest"
        redis_client.setex(cache_key, 3600, json.dumps(data))

        # Track running stats
        stats_key = f"device:{device_id}:stats"
        redis_client.hincrby(stats_key, "count", 1)
        redis_client.hincrbyfloat(stats_key, "total", reading_value)

        count = int(redis_client.hget(stats_key, "count") or 1)
        total = float(redis_client.hget(stats_key, "total") or reading_value)
        avg_reading = total / count

        processed_data = {
            "device_id": device_id,
            "reading_value": reading_value,
            "avg_reading": round(avg_reading, 4),
            "message_count": count,
        }

        redis_client.lpush("processed_queue", json.dumps(processed_data))
        producer.send(KAFKA_OUTPUT_TOPIC, processed_data)
        messages_processed += 1

        if messages_processed % 100 == 0:
            logger.info("Processed %d messages.", messages_processed)


if __name__ == "__main__":
    process_streaming_data()
