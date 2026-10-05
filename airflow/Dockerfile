# Image Airflow de la plateforme Vélib'.
# Contexte de build = racine du dépôt (le code des jobs est copié dans l'image pour Kubernetes ;
# en local, docker-compose monte les dossiers par-dessus pour le rechargement à chaud).
#
# Trois environnements Python isolés pour éviter les conflits de dépendances :
#   - l'environnement Airflow (DAGs, alertes, contrôle de fraîcheur Kafka) ;
#   - /opt/venvs/pipeline : PySpark + Delta (job silver), Great Expectations, ML (LightGBM, MLflow) ;
#   - /opt/venvs/dbt      : dbt-core + dbt-postgres.
FROM apache/airflow:3.3.2-python3.11

USER root
# Java pour PySpark (job silver exécuté en mode local dans la tâche Airflow)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    mkdir -p /opt/venvs /opt/airflow/dbt_target && chown -R airflow:0 /opt/venvs /opt/airflow/dbt_target
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY airflow/requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt

COPY airflow/requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN python -m venv /opt/venvs/pipeline && \
    /opt/venvs/pipeline/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venvs/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt

# JARs Spark nécessaires au job silver : S3A (MinIO), Delta Lake, JDBC PostgreSQL
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/venvs/pipeline/lib/python3.11/site-packages/pyspark/jars; \
    for jar in \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
      io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
      org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar; \
    do curl -fsSL -O "${MAVEN}/${jar}"; done

COPY airflow/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/venvs/dbt && \
    /opt/venvs/dbt/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# Code de la plateforme
COPY --chown=airflow:0 airflow/dags /opt/airflow/dags
COPY --chown=airflow:0 spark /opt/airflow/spark
COPY --chown=airflow:0 quality /opt/airflow/quality
COPY --chown=airflow:0 great_expectations /opt/airflow/great_expectations
COPY --chown=airflow:0 dbt /opt/airflow/dbt
COPY --chown=airflow:0 ml /opt/airflow/ml

ENV PIPELINE_PYTHON=/opt/venvs/pipeline/bin/python \
    SPARK_SUBMIT=/opt/venvs/pipeline/bin/spark-submit \
    DBT_BIN=/opt/venvs/dbt/bin/dbt \
    DBT_PROJECT_DIR=/opt/airflow/dbt/velib \
    DBT_PROFILES_DIR=/opt/airflow/dbt/velib \
    DBT_TARGET_PATH=/opt/airflow/dbt_target \
    PYTHONPATH=/opt/airflow
