"""Prédictions horaires : vélos disponibles dans 1 h pour chaque station, avec le modèle "champion".

Écrit dans ml.predictions_disponibilite ; la comparaison avec la réalité est faite dans dbt
(gold.ml_predictions_vs_reel) et affichée dans Superset.
"""

import json
import logging
import sys
from datetime import datetime, timedelta, timezone

import numpy as np

from ml.common import CHAMPION_ALIAS, EXIT_SKIP, MODEL_NAME, champion_version, mlflow_client, warehouse_engine
from ml.features import FEATURES, build_features, load_hourly

logger = logging.getLogger("ml-predict")


def main():
    import mlflow
    from psycopg2.extras import execute_values

    client = mlflow_client()
    champion = champion_version(client)
    if champion is None:
        logger.warning("Aucun modèle '%s' avec l'alias champion : prédiction ignorée.", MODEL_NAME)
        return EXIT_SKIP
    model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")

    engine = warehouse_engine()
    # 8 jours d'historique pour disposer du retard "même heure la semaine dernière"
    data = build_features(load_hourly(engine, datetime.now(timezone.utc) - timedelta(days=8)), with_target=False)
    latest = data[data["heure_fin"] == data["heure_fin"].max()].dropna(subset=["velos_t0"])
    if latest.empty:
        logger.warning("Aucune donnée récente : prédiction ignorée.")
        return EXIT_SKIP

    capacity = latest["capacite"].fillna(np.inf).to_numpy()
    predictions = np.clip(model.predict(latest[FEATURES]), 0, capacity)
    rows = [
        (int(sid), ft.to_pydatetime(), tt.to_pydatetime(), float(p), MODEL_NAME, str(champion.version))
        for sid, ft, tt, p in zip(latest["station_id"], latest["heure_fin"], latest["target_time"], predictions)
    ]
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO ml.predictions_disponibilite "
                "(station_id, feature_time, target_time, predicted_bikes, model_name, model_version) VALUES %s "
                "ON CONFLICT (station_id, target_time, model_version) DO UPDATE "
                "SET predicted_bikes = EXCLUDED.predicted_bikes, predicted_at = now()",
                rows,
            )
        conn.commit()
    finally:
        conn.close()

    summary = {
        "model_version": champion.version,
        "feature_time": latest["heure_fin"].max().isoformat(),
        "stations": len(rows),
        "mean_predicted_bikes": round(float(predictions.mean()), 2),
    }
    logger.info("Prédictions écrites : %s", summary)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
