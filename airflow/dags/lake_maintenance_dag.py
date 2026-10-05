"""Maintenance quotidienne du lake Delta : OPTIMIZE (compaction des petits fichiers créés par le
streaming toutes les minutes) puis VACUUM (suppression des fichiers obsolètes au-delà de 7 jours)."""

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from common.config import PROJECT_ROOT, SPARK_SUBMIT, START_DATE, default_args

with DAG(
    dag_id="lake_maintenance_dag",
    description="OPTIMIZE + VACUUM des tables Delta bronze et silver",
    schedule="30 2 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["velib", "lake", "maintenance"],
) as dag:
    BashOperator(
        task_id="optimize_and_vacuum",
        bash_command=(
            f"{SPARK_SUBMIT} --master local[2] --driver-memory ${{SPARK_DRIVER_MEMORY:-1g}} "
            f"--py-files {PROJECT_ROOT}/spark/lake_config.py {PROJECT_ROOT}/spark/silver_job.py "
            "--maintenance --vacuum-hours 168"
        ),
        cwd=f"{PROJECT_ROOT}/spark",
        execution_timeout=timedelta(minutes=30),
    )
