import json
import logging
import os
import random
import sys
import time
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
MESSAGE_FREQUENCY = float(os.getenv("MESSAGE_FREQUENCY", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
ACKS_MODE = os.getenv("KAFKA_ACKS_MODE", "all")
MAX_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "5"))
RETRY_BACKOFF = int(os.getenv("KAFKA_PRODUCER_RETRY_BACKOFF", "5"))


def create_producer():
    """Create Kafka producer with retry logic."""
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=ACKS_MODE,
                retries=3,
                linger_ms=10,
                batch_size=16384,
            )
            logger.info("Connected to Kafka broker: %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d). Retrying in %ds...",
                attempt,
                MAX_RETRIES,
                RETRY_BACKOFF,
            )
            time.sleep(RETRY_BACKOFF)

    logger.error("Failed to connect to Kafka after %d attempts.", MAX_RETRIES)
    sys.exit(1)


def create_kafka_topic(topic_name):
    """Create Kafka topic if it does not exist."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError, TopicAlreadyExistsError

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        existing_topics = admin_client.list_topics()

        if topic_name not in existing_topics:
            topic = NewTopic(
                name=topic_name,
                num_partitions=3,
                replication_factor=1,
            )
            admin_client.create_topics([topic])
            logger.info("Created Kafka topic '%s'.", topic_name)
        else:
            logger.info("Kafka topic '%s' already exists.", topic_name)

        admin_client.close()
    except TopicAlreadyExistsError:
        logger.info("Topic '%s' already exists.", topic_name)
    except KafkaError as e:
        logger.error("Failed to create topic '%s': %s", topic_name, e)


def generate_event():
    """Generate a random sensor reading event."""
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": int(time.time()),
        "device_id": random.randint(1000, 9999),
        "reading_value": round(random.uniform(20.0, 80.0), 2),
    }


def produce_messages():
    """Produce sensor messages to Kafka in batches."""
    producer = create_producer()
    create_kafka_topic(KAFKA_TOPIC)

    logger.info("Producing messages to topic: %s", KAFKA_TOPIC)
    batch = []
    message_count = 0

    try:
        while True:
            event = generate_event()
            batch.append(event)

            if len(batch) >= BATCH_SIZE:
                for msg in batch:
                    producer.send(KAFKA_TOPIC, msg)
                producer.flush()
                message_count += len(batch)
                logger.info(
                    "Sent batch of %d messages (total: %d).",
                    len(batch),
                    message_count,
                )
                batch.clear()

            time.sleep(MESSAGE_FREQUENCY)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    except Exception as e:
        logger.error("Producer error: %s", e)
    finally:
        if batch:
            for msg in batch:
                producer.send(KAFKA_TOPIC, msg)
            producer.flush()
            message_count += len(batch)
        producer.close()
        logger.info("Producer shutdown. Total messages sent: %d", message_count)


if __name__ == "__main__":
    produce_messages()
