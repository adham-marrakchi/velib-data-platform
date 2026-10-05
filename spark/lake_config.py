"""Configuration commune des sessions Spark : Delta Lake + accès S3A à MinIO."""

import os

LAKE_BUCKET = os.getenv("LAKE_BUCKET", "lake")
LAKE_ROOT = f"s3a://{LAKE_BUCKET}"

BRONZE_STATUS_PATH = f"{LAKE_ROOT}/bronze/velib_status"
SILVER_STATUS_PATH = f"{LAKE_ROOT}/silver/velib_status"
QUARANTINE_STATUS_PATH = f"{LAKE_ROOT}/quarantine/velib_status"


def build_spark_session(app_name, master=None, extra_conf=None):
    """Crée une SparkSession configurée pour Delta Lake sur MinIO (API S3)."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    conf = {
        # Delta Lake
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Toutes les dates en UTC ; la conversion Europe/Paris se fait dans dbt
        "spark.sql.session.timeZone": "UTC",
        # S3A -> MinIO
        "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER", "minio"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2026"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # Petits volumes : peu de partitions de shuffle
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
        "spark.databricks.delta.schema.autoMerge.enabled": "false",
    }
    conf.update(extra_conf or {})
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
