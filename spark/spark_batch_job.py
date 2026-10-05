import glob
import logging
import os
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.utils import AnalysisException

import great_expectations as ge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2024")
RAW_DATA_PATH = "s3a://raw-data/orders/orders.csv"
PROCESSED_DATA_PATH = "s3a://processed-data/orders_transformed.csv"
LOCAL_OUTPUT_DIR = "/tmp/transformed_orders"
LOCAL_OUTPUT_FILE = "/tmp/transformed_orders.csv"


def validate_schema(df):
    """Validate DataFrame schema using Great Expectations."""
    logger.info("Validating schema with Great Expectations...")
    ge_df = ge.from_pandas(df.toPandas())

    result = ge_df.expect_column_values_to_not_be_null("order_id")
    if not result.success:
        raise ValueError("Validation failed: 'order_id' contains null values")

    result = ge_df.expect_column_values_to_be_between("customer_id", min_value=1)
    if not result.success:
        raise ValueError("Validation failed: 'customer_id' is not positive")

    result = ge_df.expect_column_values_to_be_between("amount", min_value=0.01)
    if not result.success:
        raise ValueError("Validation failed: 'amount' contains zero or negative values")

    logger.info("Schema validation passed.")


def main():
    """Batch ETL: Read from MinIO, validate, transform, write back."""
    try:
        spark = (
            SparkSession.builder.appName("BatchETL")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

        logger.info("Reading raw data from MinIO (%s)...", RAW_DATA_PATH)
        df = spark.read.option("header", "true").csv(RAW_DATA_PATH)

        if df.rdd.isEmpty():
            raise RuntimeError("No data found in the input file.")

        df = (
            df.withColumn("order_id", col("order_id").cast("int"))
            .withColumn("customer_id", col("customer_id").cast("int"))
            .withColumn("amount", col("amount").cast("double"))
        )

        validate_schema(df)

        initial_count = df.count()
        logger.info("Initial record count: %d", initial_count)

        df = df.dropDuplicates(["order_id"])
        df = df.fillna({"amount": 0.0})
        df_transformed = df.withColumn("processed_timestamp", current_timestamp())

        df_transformed.groupBy("customer_id").agg(
            spark_sum("amount").alias("total_spent"),
            count("*").alias("order_count"),
        )

        final_count = df_transformed.count()
        logger.info("Final record count: %d", final_count)

        logger.info("Writing transformed data to MinIO...")
        df_transformed.write.mode("overwrite").option("header", "true").csv(PROCESSED_DATA_PATH)

        logger.info("Writing transformed data to local storage...")
        df_transformed.coalesce(1).write.mode("overwrite").option("header", "true").csv(LOCAL_OUTPUT_DIR)

        csv_files = glob.glob(f"{LOCAL_OUTPUT_DIR}/part-*.csv")
        if csv_files:
            shutil.move(csv_files[0], LOCAL_OUTPUT_FILE)
            logger.info("Transformed file written to %s", LOCAL_OUTPUT_FILE)
        else:
            logger.warning("No CSV files found after transformation.")

        spark.stop()
        logger.info("Batch ETL completed successfully.")

    except AnalysisException as e:
        logger.error("Spark analysis error: %s", str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("Batch processing failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
