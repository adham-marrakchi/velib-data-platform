"""
Feature Store stub using Feast for real-time ML feature serving.

This module demonstrates how to consume Kafka streaming data,
compute aggregate features, and store them in a Feast feature store
for real-time model inference.

Note: Requires a running Feast feature store with proper configuration.
"""

import json
import logging
import os
from datetime import datetime

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "./feature_repo")


def consume_stream_and_store_features():
    """Consume Kafka messages, compute features, and store in Feast."""
    from feast import FeatureStore

    from kafka import KafkaConsumer

    store = FeatureStore(repo_path=FEAST_REPO_PATH)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="feature_store_consumer",
    )

    logger.info("Consuming '%s' for feature extraction...", KAFKA_TOPIC)

    for message in consumer:
        data = message.value
        device_id = data.get("device_id")
        reading_value = data.get("reading_value")

        if device_id is None or reading_value is None:
            continue

        feature_data = pd.DataFrame(
            [
                {
                    "device_id": device_id,
                    "avg_reading": float(reading_value),
                    "max_reading": float(reading_value),
                    "timestamp": int(datetime.utcnow().timestamp()),
                }
            ]
        )

        try:
            store.ingest("device_features", feature_data)
            logger.info("Stored features for device %s", device_id)
        except Exception as e:
            logger.error("Failed to ingest features: %s", e)


def get_features(device_ids):
    """Fetch stored features for given device_ids from Feast."""
    from feast import FeatureStore

    store = FeatureStore(repo_path=FEAST_REPO_PATH)

    feature_refs = [
        "device_features:avg_reading",
        "device_features:max_reading",
    ]

    feature_vector = store.get_online_features(
        features=feature_refs,
        entity_rows=[{"device_id": did} for did in device_ids],
    ).to_dict()

    return feature_vector


if __name__ == "__main__":
    consume_stream_and_store_features()
