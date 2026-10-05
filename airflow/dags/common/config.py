"""Configuration partagée par les DAGs (lue depuis l'environnement)."""

import os
from datetime import datetime, timedelta

from airflow.sdk import Asset

WAREHOUSE_CONN_ID = "velib_dw"

GBFS_BASE_URL = os.getenv(
    "VELIB_GBFS_BASE_URL", "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
)
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "lake")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "velib_status")
FRESHNESS_THRESHOLD_MINUTES = int(os.getenv("FRESHNESS_THRESHOLD_MINUTES", "10"))
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")

# Exécutables des environnements isolés (cf. airflow/Dockerfile)
PIPELINE_PYTHON = os.getenv("PIPELINE_PYTHON", "/opt/venvs/pipeline/bin/python")
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "/opt/venvs/pipeline/bin/spark-submit")
DBT_BIN = os.getenv("DBT_BIN", "/opt/venvs/dbt/bin/dbt")
PROJECT_ROOT = "/opt/airflow"

# Assets : relient les DAGs producteurs et consommateurs (planification orientée données)
STATIONS_ASSET = Asset("postgres://postgres:5432/velib_dw/staging/velib_stations")
STAGING_STATUS_ASSET = Asset("postgres://postgres:5432/velib_dw/staging/velib_status")
GOLD_ASSET = Asset("postgres://postgres:5432/velib_dw/gold")
MODEL_ASSET = Asset("mlflow://mlflow:5000/models/velib_bikes_1h")

START_DATE = datetime(2026, 1, 1)


def default_args(**overrides):
    """Arguments par défaut : retries + alerte sur échec définitif."""
    from common.alerting import on_failure_callback

    args = {
        "owner": "data-platform",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": on_failure_callback,
    }
    args.update(overrides)
    return args
