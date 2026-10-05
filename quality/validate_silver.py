"""Porte qualité Great Expectations sur le lot silver (staging.velib_status_incoming).

Code de sortie : 0 = lot conforme (ou seulement des avertissements), 3 = règle critique violée.
Tous les résultats sont historisés dans monitoring.data_quality_results (affichés dans Grafana).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("quality")

SUITE_PATH = Path(os.getenv("GX_SUITES_DIR", Path(__file__).resolve().parents[1] / "great_expectations" / "suites"))
EXIT_CRITICAL = 3


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


def add_derived_columns(df, now=None):
    """Colonnes calculées utilisées par certaines attentes."""
    now = now or datetime.now(timezone.utc)
    df = df.copy()
    df["bikes_type_gap"] = (df["num_bikes_available"] - df["num_mechanical"] - df["num_ebike"]).abs()
    df["minutes_since_report"] = (now - pd.to_datetime(df["last_reported"], utc=True)).dt.total_seconds() / 60
    df["ingestion_delay_minutes"] = (
        pd.to_datetime(df["ingested_at"], utc=True) - pd.to_datetime(df["last_reported"], utc=True)
    ).dt.total_seconds() / 60
    return df


def load_suite(name="silver_velib_status"):
    with open(SUITE_PATH / f"{name}.json", encoding="utf-8") as f:
        return json.load(f)


def run_suite(df, suite_def):
    """Exécute la suite GX sur un DataFrame. Renvoie une liste de résultats simplifiés."""
    import great_expectations as gx
    import great_expectations.expectations as gxe

    context = gx.get_context(mode="ephemeral")
    batch = (
        context.data_sources.add_pandas("silver")
        .add_dataframe_asset(suite_def["name"])
        .add_batch_definition_whole_dataframe("lot")
        .get_batch(batch_parameters={"dataframe": df})
    )
    suite = gx.ExpectationSuite(name=suite_def["name"])
    for spec in suite_def["expectations"]:
        expectation_cls = getattr(gxe, spec["type"])
        suite.add_expectation(expectation_cls(**spec["kwargs"], meta={"severity": spec["severity"]}))

    validation = batch.validate(suite)
    results = []
    for r in validation.results:
        config = r.expectation_config
        kwargs = config.kwargs or {}
        results.append(
            {
                "expectation": config.type,
                "column_name": kwargs.get("column") or ",".join(kwargs.get("column_list", [])) or None,
                "severity": (config.meta or {}).get("severity", "critical"),
                "success": bool(r.success),
                "unexpected_count": (r.result or {}).get("unexpected_count"),
                "element_count": (r.result or {}).get("element_count", len(df)),
            }
        )
    return results


def main():
    engine = warehouse_engine()
    df = pd.read_sql("SELECT * FROM staging.velib_status_incoming", engine)
    suite_def = load_suite()
    logger.info("Validation de %d lignes avec la suite '%s'", len(df), suite_def["name"])

    if df.empty:
        # Lot vide : on ne bloque pas (la fraîcheur est surveillée par data_freshness_dag)
        logger.warning("Lot silver vide : aucun contrôle exécuté.")
        print(json.dumps({"rows": 0, "critical_failures": 0, "warnings": 0}))
        return 0

    results = run_suite(add_derived_columns(df), suite_def)

    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
    pd.DataFrame(results).assign(suite=suite_def["name"], dag_run_id=run_id).to_sql(
        "data_quality_results", engine, schema="monitoring", if_exists="append", index=False
    )

    critical = [r for r in results if not r["success"] and r["severity"] == "critical"]
    warnings = [r for r in results if not r["success"] and r["severity"] != "critical"]
    for r in warnings:
        logger.warning("Avertissement qualité : %s", r)
    for r in critical:
        logger.error("ÉCHEC CRITIQUE qualité : %s", r)
    print(json.dumps({"rows": len(df), "critical_failures": len(critical), "warnings": len(warnings)}))
    return EXIT_CRITICAL if critical else 0


if __name__ == "__main__":
    sys.exit(main())
