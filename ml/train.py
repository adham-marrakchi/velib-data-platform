"""Entraînement du modèle de prévision : vélos disponibles par station dans 1 heure.

- Validation TEMPORELLE : le test est la période la plus récente, jamais mélangée au passé.
- Référence : modèle naïf de persistance ("dans 1 h, comme maintenant"). Le modèle doit le battre.
- Optuna règle les hyperparamètres de LightGBM sur une validation elle aussi temporelle.
- MLflow trace paramètres, métriques et modèle ; le modèle est enregistré dans le Model Registry.
- Promotion : le nouveau modèle reçoit l'alias "champion" s'il fait au moins aussi bien que le
  champion actuel ÉVALUÉ SUR LE MÊME jeu de test.
- Les distributions de référence des features sont sauvegardées pour le calcul du drift (PSI).
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from ml.common import (
    CHAMPION_ALIAS,
    EXIT_SKIP,
    EXPERIMENT_NAME,
    MODEL_NAME,
    champion_version,
    mlflow_client,
    warehouse_engine,
)
from ml.features import DRIFT_FEATURES, FEATURES, TARGET, build_features, load_hourly, reference_bins

logger = logging.getLogger("ml-train")

TRAIN_DAYS = int(os.getenv("ML_TRAIN_DAYS", "28"))
MIN_HOURS = int(os.getenv("ML_MIN_HOURS", "6"))
TEST_SHARE = float(os.getenv("ML_TEST_SHARE", "0.2"))
OPTUNA_TRIALS = int(os.getenv("ML_OPTUNA_TRIALS", "15"))
OPTUNA_TIMEOUT = int(os.getenv("ML_OPTUNA_TIMEOUT", "300"))


def temporal_split(df, test_share):
    """Coupe sur l'axe du temps : les dernières heures (test_share) servent de test."""
    hours = np.sort(df["heure_fin"].unique())
    n_test = max(1, int(round(len(hours) * test_share)))
    cutoff = hours[-n_test]
    return df[df["heure_fin"] < cutoff], df[df["heure_fin"] >= cutoff], pd.Timestamp(cutoff)


def metrics(y_true, y_pred):
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
    }


def tune(train, n_trials, timeout):
    """Optuna sur une validation temporelle interne (dernières heures du train)."""
    fit_part, valid_part, _ = temporal_split(train, 0.2)

    def objective(trial):
        params = {
            "objective": "l1",
            "verbosity": -1,
            "n_estimators": 600,
            "learning_rate": trial.suggest_float("learning_rate", 0.02, 0.2, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 15, 127),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 200),
            "feature_fraction": trial.suggest_float("feature_fraction", 0.6, 1.0),
            "bagging_fraction": trial.suggest_float("bagging_fraction", 0.6, 1.0),
            "bagging_freq": 1,
            "lambda_l2": trial.suggest_float("lambda_l2", 1e-3, 10.0, log=True),
        }
        model = lgb.LGBMRegressor(**params)
        model.fit(
            fit_part[FEATURES],
            fit_part[TARGET],
            eval_set=[(valid_part[FEATURES], valid_part[TARGET])],
            eval_metric="l1",
            callbacks=[lgb.early_stopping(30, verbose=False)],
        )
        trial.set_user_attr("best_iteration", int(model.best_iteration_ or params["n_estimators"]))
        return mean_absolute_error(valid_part[TARGET], model.predict(valid_part[FEATURES]))

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(objective, n_trials=n_trials, timeout=timeout)
    best = dict(study.best_params)
    best.update(
        objective="l1", verbosity=-1, bagging_freq=1, n_estimators=study.best_trial.user_attrs["best_iteration"]
    )
    return best, study.best_value


def save_reference(engine, version, train):
    rows = []
    for feature in DRIFT_FEATURES:
        edges, shares = reference_bins(train[feature])
        rows.append((MODEL_NAME, str(version), feature, json.dumps(edges), json.dumps(shares)))
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "DELETE FROM ml.feature_reference WHERE model_name = %s AND model_version = %s", (MODEL_NAME, str(version))
        )
        for row in rows:
            conn.exec_driver_sql(
                "INSERT INTO ml.feature_reference (model_name, model_version, feature, bin_edges, bin_shares) "
                "VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)",
                row,
            )


def main():
    import mlflow.lightgbm
    from mlflow.models import infer_signature

    import mlflow

    engine = warehouse_engine()
    since = datetime.now(timezone.utc) - timedelta(days=TRAIN_DAYS)
    data = build_features(load_hourly(engine, since))
    data = data.dropna(subset=[TARGET, "velos_t0"])
    n_hours = data["heure_fin"].nunique()
    if n_hours < MIN_HOURS:
        logger.warning("Historique insuffisant : %d heures (minimum %d). Entraînement ignoré.", n_hours, MIN_HOURS)
        return EXIT_SKIP

    train, test, cutoff = temporal_split(data, TEST_SHARE)
    logger.info("Train : %d lignes avant %s ; test : %d lignes.", len(train), cutoff, len(test))

    client = mlflow_client()
    mlflow.set_experiment(EXPERIMENT_NAME)
    best_params, valid_mae = tune(train, OPTUNA_TRIALS, OPTUNA_TIMEOUT)

    with mlflow.start_run(run_name=f"train_{datetime.now(timezone.utc):%Y%m%d_%H%M}") as run:
        model = lgb.LGBMRegressor(**best_params)
        model.fit(train[FEATURES], train[TARGET])
        pred = np.clip(model.predict(test[FEATURES]), 0, None)

        model_metrics = metrics(test[TARGET], pred)
        baseline = metrics(test[TARGET], test["velos_t0"])  # persistance
        results = {
            "test_mae": model_metrics["mae"],
            "test_rmse": model_metrics["rmse"],
            "baseline_mae": baseline["mae"],
            "baseline_rmse": baseline["rmse"],
            "gain_vs_baseline_pct": 100 * (1 - model_metrics["mae"] / baseline["mae"]) if baseline["mae"] else 0.0,
            "optuna_valid_mae": valid_mae,
        }
        mlflow.log_params(best_params)
        mlflow.log_params(
            {
                "train_rows": len(train),
                "test_rows": len(test),
                "hours_of_history": n_hours,
                "test_cutoff": cutoff.isoformat(),
                "features": ",".join(FEATURES),
            }
        )
        mlflow.log_metrics(results)
        importance = pd.DataFrame({"feature": FEATURES, "gain": model.booster_.feature_importance("gain")})
        mlflow.log_text(importance.sort_values("gain", ascending=False).to_csv(index=False), "feature_importance.csv")

        sample = test[FEATURES].head(5)
        info = mlflow.lightgbm.log_model(
            model,
            name="model",
            signature=infer_signature(sample, model.predict(sample)),
            input_example=sample,
            registered_model_name=MODEL_NAME,
        )
        version = info.registered_model_version

        current = champion_version(client)
        promote = current is None
        if current is not None:
            champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")
            champion_mae = metrics(test[TARGET], np.clip(champion.predict(test[FEATURES]), 0, None))["mae"]
            mlflow.log_metric("champion_test_mae", champion_mae)
            promote = model_metrics["mae"] <= champion_mae
            results["champion_test_mae"] = champion_mae
        if promote:
            client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, version)
            save_reference(engine, version, train)
        mlflow.set_tags({"promoted": str(promote), "run_type": "training"})

    summary = {"run_id": run.info.run_id, "model_version": version, "promoted": promote, **results}
    logger.info("Résultat : %s", summary)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
