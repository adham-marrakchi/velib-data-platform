# Serveur MLflow : métadonnées dans PostgreSQL, artefacts dans MinIO (bucket "mlflow").
# --serve-artifacts : les clients passent par le serveur, ils n'ont pas besoin des clés S3.
FROM python:3.11-slim

RUN pip install --no-cache-dir mlflow==3.5.1 psycopg2-binary==2.9.10 boto3==1.40.45 && \
    useradd --create-home --uid 1001 mlflow

USER mlflow
EXPOSE 5000

CMD mlflow server \
    --host 0.0.0.0 --port 5000 \
    --backend-store-uri "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST:-postgres}:5432/${MLFLOW_DB:-mlflow}" \
    --artifacts-destination "s3://${MLFLOW_BUCKET:-mlflow}/artifacts" \
    --serve-artifacts \
    --allowed-hosts "*" \
    --workers 1
