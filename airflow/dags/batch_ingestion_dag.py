import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import great_expectations as ge
from airflow import DAG

logger = logging.getLogger(__name__)

# Configuration from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2024")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "raw-data")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def extract_data_from_mysql(**kwargs):
    """Extract batch data from MySQL orders table."""
    logger.info("Extracting data from MySQL...")
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    df = mysql_hook.get_pandas_df("SELECT * FROM orders")
    df.to_csv("/tmp/orders.csv", index=False)
    logger.info("Extracted %d records from MySQL.", len(df))
    return len(df)


def validate_data_with_ge(**kwargs):
    """Validate extracted data with Great Expectations."""
    logger.info("Running Great Expectations validation...")
    df = pd.read_csv("/tmp/orders.csv")
    ge_df = ge.from_pandas(df)

    result_order_id = ge_df.expect_column_values_to_not_be_null("order_id")
    if not result_order_id["success"]:
        raise ValueError("Validation failed: 'order_id' has null values")

    result_amount = ge_df.expect_column_values_to_be_between("amount", min_value=0.01, max_value=9999999)
    if not result_amount["success"]:
        raise ValueError("Validation failed: 'amount' is not strictly positive")

    logger.info("Data validation passed.")


def load_to_minio(**kwargs):
    """Upload raw CSV to MinIO S3-compatible storage."""
    logger.info("Uploading CSV to MinIO bucket '%s'...", MINIO_BUCKET_RAW)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    try:
        s3.head_bucket(Bucket=MINIO_BUCKET_RAW)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET_RAW)
        logger.info("Created bucket '%s'.", MINIO_BUCKET_RAW)

    s3.upload_file("/tmp/orders.csv", MINIO_BUCKET_RAW, "orders/orders.csv")
    logger.info("File uploaded to MinIO.")


def load_to_postgres(**kwargs):
    """Load transformed data into PostgreSQL."""
    logger.info("Loading data into PostgreSQL...")
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS orders_transformed (
            order_id INT,
            customer_id INT,
            amount DECIMAL(10,2),
            processed_timestamp TIMESTAMP
        )
    """)
    pg_hook.run("TRUNCATE TABLE orders_transformed")

    df = pd.read_csv("/tmp/transformed_orders.csv")
    rows_loaded = 0
    for _, row in df.iterrows():
        pg_hook.run(
            """INSERT INTO orders_transformed(order_id, customer_id, amount, processed_timestamp)
               VALUES (%s, %s, %s, %s)""",
            parameters=(
                row["order_id"],
                row["customer_id"],
                row["amount"],
                row["processed_timestamp"],
            ),
        )
        rows_loaded += 1

    logger.info("Loaded %d records into PostgreSQL.", rows_loaded)


with DAG(
    dag_id="batch_ingestion_dag",
    default_args=default_args,
    description="Batch ETL: MySQL -> Validation -> MinIO -> Spark -> PostgreSQL",
    schedule="@daily",
    catchup=False,
    tags=["batch", "etl"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_data_from_mysql,
        execution_timeout=timedelta(minutes=10),
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data_with_ge,
        execution_timeout=timedelta(minutes=5),
    )

    load_to_minio_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio,
        execution_timeout=timedelta(minutes=10),
    )

    spark_transform_task = BashOperator(
        task_id="spark_transform",
        bash_command="spark-submit --master local[2] /opt/spark_jobs/spark_batch_job.py",
        execution_timeout=timedelta(minutes=30),
    )

    load_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        execution_timeout=timedelta(minutes=15),
    )

    (extract_task >> validate_task >> load_to_minio_task >> spark_transform_task >> load_postgres_task)
