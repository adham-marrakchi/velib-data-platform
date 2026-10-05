"""Fraîcheur des données : alerte si aucun message Vélib' n'est arrivé dans Kafka depuis N minutes.

On lit l'horodatage du DERNIER message de chaque partition du topic (sans consumer group, donc
sans effet de bord sur les consommateurs réels).
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from common.config import FRESHNESS_THRESHOLD_MINUTES, KAFKA_BROKER, KAFKA_TOPIC, START_DATE, default_args


def latest_message_time(broker=KAFKA_BROKER, topic=KAFKA_TOPIC):
    """Horodatage (UTC) du message le plus récent du topic, ou None si le topic est vide."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {"bootstrap.servers": broker, "group.id": "freshness-probe", "enable.auto.commit": False}
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        if topic not in metadata.topics or metadata.topics[topic].error:
            raise RuntimeError(f"Topic {topic} introuvable")
        latest = None
        for partition in metadata.topics[topic].partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high <= low:
                continue
            consumer.assign([TopicPartition(topic, partition, high - 1)])
            msg = consumer.poll(10)
            if msg is None or msg.error():
                continue
            ts = datetime.fromtimestamp(msg.timestamp()[1] / 1000, tz=timezone.utc)
            latest = ts if latest is None or ts > latest else latest
        return latest
    finally:
        consumer.close()


def check_freshness():
    from common.alerting import AlertedFailure, notify

    latest = latest_message_time()
    now = datetime.now(timezone.utc)
    age_minutes = None if latest is None else (now - latest).total_seconds() / 60
    if age_minutes is not None and age_minutes <= FRESHNESS_THRESHOLD_MINUTES:
        return {"latest_message": latest.isoformat(), "age_minutes": round(age_minutes, 1)}

    detail = "aucun message dans le topic" if latest is None else f"dernier message il y a {age_minutes:.0f} min"
    notify(
        "freshness",
        "critical",
        title=f"Plus de données Vélib' depuis {FRESHNESS_THRESHOLD_MINUTES} min",
        message=f"Topic {KAFKA_TOPIC} : {detail}. Vérifier le producteur et l'API GBFS.",
        source="data_freshness_dag",
    )
    raise AlertedFailure(f"Données périmées : {detail}")


with DAG(
    dag_id="data_freshness_dag",
    description=f"Alerte si aucune donnée Kafka depuis {FRESHNESS_THRESHOLD_MINUTES} minutes",
    schedule="*/5 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=0),
    tags=["velib", "monitoring", "alerting"],
) as dag:
    PythonOperator(
        task_id="check_kafka_freshness",
        python_callable=check_freshness,
        execution_timeout=timedelta(minutes=2),
    )
