-- =============================================================
-- Warehouse velib_dw : schémas, tables d'atterrissage et droits
-- Variables psql : :owner, :gold_reader, :assistant
-- =============================================================

CREATE SCHEMA IF NOT EXISTS staging;     -- données silver chargées depuis le lake (source de dbt)
CREATE SCHEMA IF NOT EXISTS gold;        -- modèles dbt (étoile + agrégats), exposé à la BI et à l'IA
CREATE SCHEMA IF NOT EXISTS ml;          -- prédictions, références de features, drift
CREATE SCHEMA IF NOT EXISTS monitoring;  -- résultats qualité et alertes

-- -------------------------------------------------------------
-- staging : relevés silver (une ligne par station et par relevé)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.velib_status (
    station_id           BIGINT      NOT NULL,
    station_code         TEXT,
    last_reported        TIMESTAMPTZ NOT NULL,
    num_bikes_available  INTEGER     NOT NULL,
    num_mechanical       INTEGER     NOT NULL,
    num_ebike            INTEGER     NOT NULL,
    num_docks_available  INTEGER     NOT NULL,
    is_installed         BOOLEAN,
    is_renting           BOOLEAN,
    is_returning         BOOLEAN,
    kafka_timestamp      TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, last_reported)
);
CREATE INDEX IF NOT EXISTS idx_velib_status_loaded_at ON staging.velib_status (loaded_at);
CREATE INDEX IF NOT EXISTS idx_velib_status_last_reported ON staging.velib_status (last_reported);

-- Table tampon écrasée à chaque exécution du job silver, validée par Great Expectations
-- avant fusion dans staging.velib_status.
CREATE TABLE IF NOT EXISTS staging.velib_status_incoming (LIKE staging.velib_status INCLUDING DEFAULTS);
ALTER TABLE staging.velib_status_incoming ALTER COLUMN loaded_at DROP NOT NULL;

-- Référentiel des stations (SCD type 1 + dates de première / dernière apparition)
CREATE TABLE IF NOT EXISTS staging.velib_stations (
    station_id     BIGINT PRIMARY KEY,
    station_code   TEXT,
    station_name   TEXT NOT NULL,
    lat            DOUBLE PRECISION NOT NULL,
    lon            DOUBLE PRECISION NOT NULL,
    capacity       INTEGER,
    commune        TEXT,
    code_insee     TEXT,
    first_seen     DATE NOT NULL DEFAULT current_date,
    last_seen      DATE NOT NULL DEFAULT current_date,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ml : sorties du modèle de prévision
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ml.predictions_disponibilite (
    station_id        BIGINT      NOT NULL,
    feature_time      TIMESTAMPTZ NOT NULL,   -- heure des dernières données connues
    target_time       TIMESTAMPTZ NOT NULL,   -- heure prédite (feature_time + 1 h)
    predicted_bikes   DOUBLE PRECISION NOT NULL,
    model_name        TEXT NOT NULL,
    model_version     TEXT NOT NULL,
    predicted_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, target_time, model_version)
);

CREATE TABLE IF NOT EXISTS ml.feature_reference (
    model_name     TEXT NOT NULL,
    model_version  TEXT NOT NULL,
    feature        TEXT NOT NULL,
    bin_edges      JSONB NOT NULL,
    bin_shares     JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (model_name, model_version, feature)
);

CREATE TABLE IF NOT EXISTS ml.drift_psi (
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    model_name     TEXT NOT NULL,
    model_version  TEXT NOT NULL,
    feature        TEXT NOT NULL,
    psi            DOUBLE PRECISION NOT NULL,
    threshold      DOUBLE PRECISION NOT NULL,
    is_drift       BOOLEAN NOT NULL,
    n_current      INTEGER NOT NULL
);

-- -------------------------------------------------------------
-- monitoring : qualité des données et alertes
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.data_quality_results (
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    suite           TEXT NOT NULL,
    expectation     TEXT NOT NULL,
    column_name     TEXT,
    severity        TEXT NOT NULL,
    success         BOOLEAN NOT NULL,
    unexpected_count BIGINT,
    element_count   BIGINT,
    dag_run_id      TEXT
);

CREATE TABLE IF NOT EXISTS monitoring.alerts (
    alert_id     BIGSERIAL PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    alert_type   TEXT NOT NULL,     -- dag_failure, data_quality, drift, freshness
    severity     TEXT NOT NULL,     -- info, warning, critical
    title        TEXT NOT NULL,
    message      TEXT,
    source       TEXT,
    delivered_to TEXT[]
);

-- -------------------------------------------------------------
-- Droits : lecture seule sur gold (+ ml/monitoring pour la BI)
-- -------------------------------------------------------------
GRANT USAGE ON SCHEMA gold TO :gold_reader, :assistant;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO :gold_reader, :assistant;
ALTER DEFAULT PRIVILEGES FOR ROLE :owner IN SCHEMA gold GRANT SELECT ON TABLES TO :gold_reader, :assistant;

GRANT USAGE ON SCHEMA ml, monitoring TO :gold_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ml, monitoring TO :gold_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE :owner IN SCHEMA ml, monitoring GRANT SELECT ON TABLES TO :gold_reader;

-- L'assistant IA : uniquement gold, transactions en lecture seule, délai maximal de 10 s
ALTER ROLE :assistant SET default_transaction_read_only = on;
ALTER ROLE :assistant SET statement_timeout = '10s';
ALTER ROLE :assistant SET search_path = gold;
ALTER ROLE :gold_reader SET default_transaction_read_only = on;
ALTER ROLE :gold_reader SET statement_timeout = '60s';

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
