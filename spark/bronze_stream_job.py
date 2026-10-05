"""BRONZE — Spark Structured Streaming : Kafka (velib_status) -> Delta Lake brut sur MinIO.

- On stocke le JSON tel quel + les métadonnées Kafka (topic, partition, offset, timestamp).
  Rien n'est perdu : silver pourra toujours être recalculé depuis bronze.
- Garantie exactly-once : le checkpoint mémorise les offsets Kafka traités et le sink Delta est
  idempotent (chaque micro-batch est écrit dans une transaction identifiée par son batchId).
  Après un crash, Spark reprend au dernier offset validé sans doublon.
- Partitionnement par date Kafka (déterministe en cas de rejeu, contrairement à l'heure d'ingestion).
- Un listener expose lag, débit et durée des micro-batchs à Prometheus (:8001/metrics).
"""

import logging
import os

from lake_config import BRONZE_STATUS_PATH, build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("bronze-stream")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "velib_status")
CHECKPOINT_DIR = os.getenv("BRONZE_CHECKPOINT_DIR", "/opt/spark_checkpoints/bronze_velib_status")
TRIGGER_INTERVAL = os.getenv("BRONZE_TRIGGER_INTERVAL", "60 seconds")
MAX_OFFSETS_PER_TRIGGER = os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER", "200000")
METRICS_PORT = int(os.getenv("BRONZE_METRICS_PORT", "8001"))


def to_bronze(df_kafka):
    """Projection bronze : JSON brut + métadonnées Kafka + date de partition."""
    from pyspark.sql import functions as F

    return df_kafka.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date(F.col("timestamp")).alias("ingest_date"),
    )


def register_metrics_listener(spark):
    """Publie la progression du stream (débit, durée, lag Kafka) en métriques Prometheus."""
    from prometheus_client import Gauge, start_http_server
    from pyspark.sql.streaming import StreamingQueryListener

    start_http_server(METRICS_PORT)
    input_rows = Gauge("velib_bronze_input_rows", "Lignes lues dans le dernier micro-batch")
    input_rate = Gauge("velib_bronze_input_rows_per_second", "Débit d'entrée (lignes/s)")
    process_rate = Gauge("velib_bronze_processed_rows_per_second", "Débit de traitement (lignes/s)")
    batch_duration = Gauge("velib_bronze_batch_duration_ms", "Durée totale du dernier micro-batch")
    offsets_behind = Gauge("velib_bronze_kafka_offsets_behind_latest", "Lag Kafka : offsets non encore lus")
    last_progress = Gauge("velib_bronze_last_progress_timestamp", "Horodatage du dernier micro-batch")

    class PrometheusListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            logger.info("Stream démarré : %s", event.id)

        def onQueryProgress(self, event):
            progress = event.progress
            input_rows.set(progress.numInputRows)
            input_rate.set(progress.inputRowsPerSecond or 0)
            process_rate.set(progress.processedRowsPerSecond or 0)
            batch_duration.set((progress.durationMs or {}).get("triggerExecution", 0))
            lag = 0
            for source in progress.sources:
                metrics = source.metrics or {}
                lag += float(metrics.get("maxOffsetsBehindLatest", 0) or 0)
            offsets_behind.set(lag)
            last_progress.set_to_current_time()

        def onQueryIdle(self, event):
            last_progress.set_to_current_time()

        def onQueryTerminated(self, event):
            logger.warning("Stream terminé : %s (exception=%s)", event.id, event.exception)

    spark.streams.addListener(PrometheusListener())


def main():
    spark = build_spark_session(
        "velib-bronze-stream",
        master=os.getenv("SPARK_STREAMING_MASTER", "local[2]"),
        extra_conf={
            "spark.sql.streaming.metricsEnabled": "true",
            "spark.ui.prometheus.enabled": "true",
            "spark.metrics.namespace": "velib_bronze",
        },
    )
    spark.sparkContext.setLogLevel("WARN")
    register_metrics_listener(spark)

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        to_bronze(df_kafka)
        .writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("velib_bronze")
        .start(BRONZE_STATUS_PATH)
    )
    logger.info("Streaming %s -> %s (checkpoint %s)", KAFKA_TOPIC, BRONZE_STATUS_PATH, CHECKPOINT_DIR)
    query.awaitTermination()


if __name__ == "__main__":
    main()
