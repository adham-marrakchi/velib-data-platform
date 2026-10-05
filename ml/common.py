"""Utilitaires partagés par l'entraînement, la prédiction et le suivi de drift."""

import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "velib_bikes_1h")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "velib_disponibilite_1h")
CHAMPION_ALIAS = "champion"
EXIT_SKIP = 99  # interprété par Airflow comme "tâche ignorée" (BashOperator.skip_on_exit_code)


def warehouse_engine():
    from sqlalchemy import create_engine

    url = "postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}".format(
        u=os.getenv("POSTGRES_USER", "velib"),
        p=os.getenv("POSTGRES_PASSWORD", "velib_secret_2026"),
        h=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "velib_dw"),
    )
    return create_engine(url)


def mlflow_client():
    import mlflow
    from mlflow import MlflowClient

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    return MlflowClient()


def champion_version(client):
    """Version du modèle portant l'alias 'champion', ou None."""
    from mlflow.exceptions import MlflowException

    try:
        return client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
    except MlflowException:
        return None
