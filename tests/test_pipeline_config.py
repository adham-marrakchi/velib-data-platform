"""Tests de structure : les fichiers clés de la plateforme sont présents."""

import os

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")


@pytest.mark.parametrize(
    "path",
    [
        ".env.example",
        "docker-compose.yaml",
        ".dockerignore",
        "Makefile",
        "kafka/producer.py",
        "spark/bronze_stream_job.py",
        "spark/silver_job.py",
        "spark/lake_config.py",
        "quality/validate_silver.py",
        "ml/train.py",
        "ml/predict.py",
        "ml/drift_psi.py",
        "dbt/velib/dbt_project.yml",
        "monitoring/prometheus.yml",
    ],
)
def test_fichier_present(path):
    assert os.path.exists(os.path.join(ROOT, path)), f"{path} introuvable"


@pytest.mark.parametrize(
    "dag",
    [
        "velib_stations_reference_dag.py",
        "silver_quality_dag.py",
        "dbt_gold_dag.py",
        "data_freshness_dag.py",
        "lake_maintenance_dag.py",
        "ml_dags.py",
    ],
)
def test_dag_present(dag):
    assert os.path.exists(os.path.join(ROOT, "airflow", "dags", dag))


@pytest.mark.parametrize(
    "dockerfile", ["airflow/Dockerfile", "spark/Dockerfile", "kafka/Dockerfile", "mlflow/Dockerfile"]
)
def test_dockerfile_present(dockerfile):
    assert os.path.exists(os.path.join(ROOT, dockerfile))


@pytest.mark.parametrize("layer", ["staging", "intermediate", "marts"])
def test_couches_dbt(layer):
    models = os.path.join(ROOT, "dbt", "velib", "models", layer)
    assert any(f.endswith(".sql") for f in os.listdir(models)), f"aucun modèle dbt dans {layer}"
