import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2024")
RAW_DATA_PATH = "s3a://raw-data/streaming_raw/"
ANOMALY_DATA_PATH = "s3a://processed-data/streaming_anomalies/"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "processed_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_secret_2024")
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "70.0"))

schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("device_id", LongType(), False),
        StructField("reading_value", DoubleType(), False),
    ]
)


def save_to_postgres(batch_df, batch_id):
    """Write a micro-batch to PostgreSQL with error handling."""
    try:
        if batch_df.rdd.isEmpty():
            return
        row_count = batch_df.count()
        logger.info("Writing batch %d to PostgreSQL (%d rows)...", batch_id, row_count)
        (
            batch_df.write.format("jdbc")
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
            .option("dbtable", "anomalies_stream")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        logger.info("Batch %d written to PostgreSQL (%d rows).", batch_id, row_count)
    except Exception as e:
        logger.error("Failed to write batch %d to PostgreSQL: %s", batch_id, str(e))


def main():
    """Streaming ETL: Kafka -> Parse -> Filter -> Detect anomalies -> Write."""
    try:
        spark = (
            SparkSession.builder.appName("StreamingETL")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.streaming.checkpointLocation", "/opt/spark_checkpoints")
            .getOrCreate()
        )

        logger.info("Starting Spark Structured Streaming from Kafka topic: %s", KAFKA_TOPIC)

        df_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        df_clean = df_parsed.filter(
            col("event_id").isNotNull()
            & col("timestamp").isNotNull()
            & col("device_id").isNotNull()
            & col("reading_value").isNotNull()
        ).withColumn("processed_at", current_timestamp())

        df_anomalies = df_clean.filter(col("reading_value") > ANOMALY_THRESHOLD)

        # Write clean data to MinIO as Parquet
        (
            df_clean.writeStream.format("parquet")
            .option("checkpointLocation", "/opt/spark_checkpoints/raw")
            .option("path", RAW_DATA_PATH)
            .outputMode("append")
            .queryName("raw_to_minio")
            .start()
        )

        # Write anomalies to MinIO
        (
            df_anomalies.writeStream.format("parquet")
            .option("checkpointLocation", "/opt/spark_checkpoints/anomalies")
            .option("path", ANOMALY_DATA_PATH)
            .outputMode("append")
            .queryName("anomalies_to_minio")
            .start()
        )

        # Write anomalies to PostgreSQL
        (
            df_anomalies.writeStream.foreachBatch(save_to_postgres)
            .outputMode("append")
            .option("checkpointLocation", "/opt/spark_checkpoints/anomalies-pg")
            .queryName("anomalies_to_postgres")
            .start()
        )

        logger.info("Streaming job started. Processing events in real-time.")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error("Streaming processing failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
