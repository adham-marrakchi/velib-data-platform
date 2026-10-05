#!/bin/bash
# Initialisation PostgreSQL (exécuté une seule fois, au premier démarrage du volume).
#  - une base par outil (airflow, mlflow, superset) pour ne pas mélanger métadonnées et données ;
#  - le warehouse velib_dw avec ses schémas (staging, gold, ml, monitoring) ;
#  - deux rôles en LECTURE SEULE sur gold : un pour Superset, un pour l'assistant IA.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE ${AIRFLOW_DB:-airflow};
  CREATE DATABASE ${MLFLOW_DB:-mlflow};
  CREATE DATABASE ${SUPERSET_DB:-superset};

  CREATE ROLE ${GOLD_READER_USER:-gold_reader} LOGIN PASSWORD '${GOLD_READER_PASSWORD:-gold_reader_2026}';
  CREATE ROLE ${ASSISTANT_DB_USER:-assistant_ro} LOGIN PASSWORD '${ASSISTANT_DB_PASSWORD:-assistant_ro_2026}';
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -f /docker-entrypoint-initdb.d/sql/warehouse.sql \
  -v gold_reader="${GOLD_READER_USER:-gold_reader}" \
  -v assistant="${ASSISTANT_DB_USER:-assistant_ro}" \
  -v owner="$POSTGRES_USER"

echo "Initialisation du warehouse terminée."
