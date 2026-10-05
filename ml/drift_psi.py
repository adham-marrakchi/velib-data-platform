"""Détection de drift : PSI (Population Stability Index) des features récentes vs entraînement.

Pour chaque feature surveillée, on répartit les valeurs des dernières 24 h dans les déciles calculés
sur les données d'entraînement du modèle champion, puis :
    PSI = somme sur les classes de (part_actuelle - part_reference) * ln(part_actuelle / part_reference)
Lecture usuelle : < 0,1 stable ; 0,1-0,2 à surveiller ; > 0,2 changement significatif -> réentraînement.
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

from ml.common import EXIT_SKIP, MODEL_NAME, champion_version, mlflow_client, warehouse_engine
from ml.features import DRIFT_FEATURES, bin_shares, build_features, load_hourly, psi

logger = logging.getLogger("ml-drift")
PSI_THRESHOLD = float(os.getenv("PSI_THRESHOLD", "0.2"))
WINDOW_HOURS = int(os.getenv("DRIFT_WINDOW_HOURS", "24"))


def main():
    champion = champion_version(mlflow_client())
    if champion is None:
        logger.warning("Pas de modèle champion : drift non calculé.")
        return EXIT_SKIP

    engine = warehouse_engine()
    reference = pd.read_sql(
        "SELECT feature, bin_edges, bin_shares FROM ml.feature_reference WHERE model_name = %(m)s AND model_version = %(v)s",
        engine,
        params={"m": MODEL_NAME, "v": str(champion.version)},
    )
    if reference.empty:
        logger.warning("Pas de distribution de référence pour la version %s.", champion.version)
        return EXIT_SKIP

    now = datetime.now(timezone.utc)
    # 2 h de marge pour pouvoir calculer les retards (lags) des premières heures de la fenêtre
    current = build_features(load_hourly(engine, now - timedelta(hours=WINDOW_HOURS + 2)), with_target=False)
    current = current[current["heure_fin"] >= now - timedelta(hours=WINDOW_HOURS)]

    results = []
    for row in reference.itertuples():
        values = current[row.feature].dropna()
        score = psi(row.bin_shares, bin_shares(values, row.bin_edges)) if len(values) else float("nan")
        results.append(
            {
                "model_name": MODEL_NAME,
                "model_version": str(champion.version),
                "feature": row.feature,
                "psi": score,
                "threshold": PSI_THRESHOLD,
                "is_drift": bool(score > PSI_THRESHOLD),
                "n_current": int(len(values)),
            }
        )
    pd.DataFrame(results).to_sql("drift_psi", engine, schema="ml", if_exists="append", index=False)

    drifted = [r["feature"] for r in results if r["is_drift"]]
    summary = {
        "model_version": champion.version,
        "max_psi": round(max(r["psi"] for r in results), 4),
        "drift": bool(drifted),
        "features_in_drift": drifted,
        "threshold": PSI_THRESHOLD,
    }
    logger.info("PSI : %s", {r["feature"]: round(r["psi"], 4) for r in results})
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
