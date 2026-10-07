"""GOLD : modèles dbt (étoile + agrégats) dans PostgreSQL, puis tests et documentation.

Horaire : fraîcheur des sources -> seed -> run -> test -> docs. Un test dbt en échec fait échouer
le DAG (alerte) et la documentation n'est pas régénérée.
"""

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from common.config import DBT_BIN, GOLD_ASSET, START_DATE, default_args

DBT = f"{DBT_BIN} --no-use-colors"
DBT_FLAGS = "--project-dir $DBT_PROJECT_DIR --profiles-dir $DBT_PROFILES_DIR --target-path $DBT_TARGET_PATH"
DBT_ENV = {"DBT_LOG_PATH": "/tmp/dbt_logs", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}

with DAG(
    dag_id="dbt_gold_dag",
    description="dbt : staging -> marts gold (étoile + KPIs), tests et documentation",
    schedule="5 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["velib", "gold", "dbt"],
) as dag:

    def dbt_task(task_id, command, **kwargs):
        return BashOperator(
            task_id=task_id,
            bash_command=f"{DBT} {command} {DBT_FLAGS}",
            env=DBT_ENV,
            append_env=True,
            execution_timeout=timedelta(minutes=20),
            **kwargs,
        )

    freshness = dbt_task("dbt_source_freshness", "source freshness", retries=0)
    seed = dbt_task("dbt_seed", "seed")
    run = dbt_task("dbt_run", "run")
    test = dbt_task("dbt_test", "test", outlets=[GOLD_ASSET])
    docs = dbt_task("dbt_docs_generate", "docs generate")

    # La fraîcheur est informative : un retard de données ne bloque pas la reconstruction du gold
    freshness.trigger_rule = "all_done"
    seed.trigger_rule = "all_done"
    freshness >> seed >> run >> test >> docs
