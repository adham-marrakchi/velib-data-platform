"""Référentiel quotidien des stations Vélib' (nom, coordonnées, capacité, commune)."""

from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from common.config import START_DATE, STATIONS_ASSET, default_args
from common.stations import fetch_to_bronze, upsert_stations

with DAG(
    dag_id="velib_stations_reference_dag",
    description="station_information.json -> MinIO (bronze) -> staging.velib_stations",
    schedule="0 3 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["velib", "bronze", "reference"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_station_information_to_bronze",
        python_callable=fetch_to_bronze,
        execution_timeout=timedelta(minutes=5),
    )
    load = PythonOperator(
        task_id="upsert_stations",
        python_callable=upsert_stations,
        op_args=[fetch.output],
        outlets=[STATIONS_ASSET],
        execution_timeout=timedelta(minutes=30),
    )
    fetch >> load
