"""SILVER : bronze Delta -> silver Delta -> contrôle qualité GX -> staging PostgreSQL.

Toutes les 15 minutes :
  1. vide la table tampon staging.velib_status_incoming ;
  2. job Spark silver sur la fenêtre [data_interval_start - 30 min, data_interval_end[ ;
  3. suite Great Expectations : une règle critique violée fait ÉCHOUER la tâche (alerte data_quality)
     et le lot n'est PAS fusionné ;
  4. fusion idempotente dans staging.velib_status (ON CONFLICT DO NOTHING).
"""

from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from common.alerting import on_quality_failure
from common.config import (
    PIPELINE_PYTHON,
    PROJECT_ROOT,
    SPARK_SUBMIT,
    STAGING_STATUS_ASSET,
    START_DATE,
    WAREHOUSE_CONN_ID,
    default_args,
)

SPARK_CMD = (
    f"{SPARK_SUBMIT} --master local[2] --driver-memory ${{SPARK_DRIVER_MEMORY:-1g}} "
    '--driver-java-options "-Duser.timezone=UTC" '
    f"--py-files {PROJECT_ROOT}/spark/lake_config.py {PROJECT_ROOT}/spark/silver_job.py "
    "--start {{ data_interval_start.isoformat() }} --end {{ data_interval_end.isoformat() }} "
    "--lookback-minutes 30 2>/tmp/silver_spark_stderr.log | tail -n 1"
)

with DAG(
    dag_id="velib_silver_quality_dag",
    description="Bronze -> silver (Delta) -> qualité GX -> staging PostgreSQL",
    schedule="*/15 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    default_args=default_args(retries=1),
    tags=["velib", "silver", "quality"],
) as dag:
    truncate_incoming = SQLExecuteQueryOperator(
        task_id="truncate_incoming",
        conn_id=WAREHOUSE_CONN_ID,
        sql="TRUNCATE staging.velib_status_incoming;",
    )

    silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=f"set -o pipefail; {SPARK_CMD}",
        cwd=f"{PROJECT_ROOT}/spark",
        execution_timeout=timedelta(minutes=10),
    )

    quality = BashOperator(
        task_id="validate_silver_quality",
        bash_command=f"set -o pipefail; {PIPELINE_PYTHON} -m quality.validate_silver | tail -n 1",
        append_env=True,
        env={"PYTHONPATH": PROJECT_ROOT},
        cwd=PROJECT_ROOT,
        retries=0,
        on_failure_callback=on_quality_failure,
        execution_timeout=timedelta(minutes=5),
    )

    merge = SQLExecuteQueryOperator(
        task_id="merge_into_staging",
        conn_id=WAREHOUSE_CONN_ID,
        sql="""
            INSERT INTO staging.velib_status
                (station_id, station_code, last_reported, num_bikes_available, num_mechanical, num_ebike,
                 num_docks_available, is_installed, is_renting, is_returning, kafka_timestamp, ingested_at)
            SELECT station_id, station_code, last_reported, num_bikes_available, num_mechanical, num_ebike,
                   num_docks_available, is_installed, is_renting, is_returning, kafka_timestamp, ingested_at
            FROM staging.velib_status_incoming
            ON CONFLICT (station_id, last_reported) DO NOTHING;
        """,
        outlets=[STAGING_STATUS_ASSET],
    )

    truncate_incoming >> silver >> quality >> merge
