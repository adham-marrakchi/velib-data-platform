"""DAGs ML : entraînement (hebdomadaire ou sur drift), prédiction (après chaque mise à jour du gold),
surveillance du drift (quotidienne, PSI > seuil -> alerte + réentraînement).

Les scripts tournent dans le venv "pipeline" ; le code de sortie 99 signifie "rien à faire"
(historique insuffisant, pas encore de modèle) et marque la tâche comme ignorée.
"""

import json
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

from common.config import GOLD_ASSET, MODEL_ASSET, PIPELINE_PYTHON, PROJECT_ROOT, START_DATE, default_args


def ml_task(task_id, module, **kwargs):
    return BashOperator(
        task_id=task_id,
        bash_command=f"set -o pipefail; {PIPELINE_PYTHON} -m ml.{module} | tail -n 1",
        cwd=PROJECT_ROOT,
        env={"PYTHONPATH": PROJECT_ROOT},
        append_env=True,
        skip_on_exit_code=[99],
        **kwargs,
    )


with DAG(
    dag_id="ml_training_dag",
    description="Entraînement LightGBM + Optuna, suivi MLflow, promotion du champion",
    schedule="0 4 * * 1",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=1),
    tags=["velib", "ml", "training"],
) as training_dag:
    ml_task("train_and_register", "train", outlets=[MODEL_ASSET], execution_timeout=timedelta(minutes=45))


with DAG(
    dag_id="ml_prediction_dag",
    description="Prévision à 1 h pour chaque station avec le modèle champion",
    schedule=[GOLD_ASSET],
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=1),
    tags=["velib", "ml", "prediction"],
) as prediction_dag:
    ml_task("predict_next_hour", "predict", execution_timeout=timedelta(minutes=15))


def drift_detected(ti):
    """Lit le résumé JSON du calcul PSI ; continue (True) seulement en cas de drift."""
    raw = ti.xcom_pull(task_ids="compute_psi")
    summary = json.loads(raw) if raw else {}
    return bool(summary.get("drift"))


def alert_drift(ti):
    from common.alerting import notify

    summary = json.loads(ti.xcom_pull(task_ids="compute_psi"))
    notify(
        "drift",
        "warning",
        title=f"Drift détecté sur le modèle v{summary['model_version']}",
        message=(
            f"PSI max = {summary['max_psi']} (seuil {summary['threshold']}) sur : "
            f"{', '.join(summary['features_in_drift'])}. Réentraînement déclenché."
        ),
        source="drift_monitoring_dag",
        dedup_minutes=0,
    )


with DAG(
    dag_id="drift_monitoring_dag",
    description="PSI quotidien des features ; drift -> alerte + réentraînement",
    schedule="0 6 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=1),
    tags=["velib", "ml", "monitoring"],
) as drift_dag:
    compute = ml_task("compute_psi", "drift_psi", execution_timeout=timedelta(minutes=10))
    is_drift = ShortCircuitOperator(task_id="drift_detected", python_callable=drift_detected)
    alert = PythonOperator(task_id="alert_drift", python_callable=alert_drift)
    retrain = TriggerDagRunOperator(task_id="trigger_retraining", trigger_dag_id="ml_training_dag")
    compute >> is_drift >> alert >> retrain
