import logging
import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
CONSUMER_GROUP = "sensor_readings_monitor"
LAG_THRESHOLD = int(os.getenv("KAFKA_LAG_THRESHOLD", "100"))


def check_kafka_health(**kwargs):
    """Check Kafka broker connectivity and topic status."""
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError

    try:
        logger.info("Connecting to Kafka broker: %s", KAFKA_BROKER)
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=10000,
        )

        topics = admin_client.list_topics()
        logger.info("Available Kafka topics: %s", topics)

        if KAFKA_TOPIC not in topics:
            logger.warning("Topic '%s' not found in Kafka.", KAFKA_TOPIC)

        admin_client.close()
        logger.info("Kafka health check passed.")

    except KafkaError as e:
        logger.error("Kafka connection error: %s", str(e))
        raise
    except Exception as e:
        logger.error("Kafka monitoring error: %s", str(e))
        raise


def check_kafka_consumer_lag(**kwargs):
    """Monitor consumer group lag for the sensor_readings topic."""
    from kafka import KafkaConsumer, TopicPartition
    from kafka.errors import KafkaError

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,
            request_timeout_ms=10000,
        )

        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            logger.warning("No partitions found for topic '%s'.", KAFKA_TOPIC)
            consumer.close()
            return

        total_lag = 0
        for partition in partitions:
            tp = TopicPartition(KAFKA_TOPIC, partition)
            consumer.assign([tp])
            consumer.seek_to_end(tp)
            end_offset = consumer.position(tp)
            committed = consumer.committed(tp)
            committed_offset = committed if committed is not None else 0

            lag = end_offset - committed_offset
            total_lag += lag
            logger.info(
                "Partition %d: end=%d, committed=%d, lag=%d",
                partition,
                end_offset,
                committed_offset,
                lag,
            )

        if total_lag > LAG_THRESHOLD:
            logger.warning(
                "Total consumer lag (%d) exceeds threshold (%d)!",
                total_lag,
                LAG_THRESHOLD,
            )

        consumer.close()
        logger.info("Consumer lag monitoring complete. Total lag: %d", total_lag)

    except KafkaError as e:
        logger.error("Kafka consumer lag check failed: %s", str(e))
        raise


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="streaming_monitoring_dag",
    default_args=default_args,
    description="Monitor Kafka streaming health and consumer lag",
    schedule="*/15 * * * *",
    catchup=False,
    tags=["streaming", "monitoring"],
) as dag:
    health_check = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
        execution_timeout=timedelta(minutes=5),
    )

    lag_check = PythonOperator(
        task_id="check_kafka_consumer_lag",
        python_callable=check_kafka_consumer_lag,
        execution_timeout=timedelta(minutes=5),
    )

    health_check >> lag_check
