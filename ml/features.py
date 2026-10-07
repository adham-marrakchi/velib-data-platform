"""Construction des features à partir de la série horaire gold.fct_station_horaire.

Convention temporelle (évite toute fuite de données) :
  - une ligne = une station à l'instant t (fin d'heure) ; on ne connaît que le passé jusqu'à t ;
  - la cible est le nombre de vélos à t + 1 h ;
  - les variables calendaires décrivent l'heure CIBLE (connue à l'avance, donc légitime) ;
  - les retards (lags) sont calculés par jointure sur l'horodatage, pas par position de ligne :
    un trou dans la série donne une valeur manquante, jamais une valeur décalée.
"""

import holidays
import numpy as np
import pandas as pd

TIMEZONE = "Europe/Paris"
HORIZON = pd.Timedelta(hours=1)
MAX_STALENESS_MINUTES = 24 * 60  # station muette depuis plus de 24 h : exclue

FEATURES = [
    "heure_cible",
    "jour_semaine_cible",
    "est_weekend_cible",
    "est_ferie_cible",
    "velos_t0",
    "velos_lag_1h",
    "velos_lag_2h",
    "velos_cible_moins_24h",
    "velos_cible_moins_7j",
    "moyenne_3h",
    "tendance_1h",
    "part_electriques",
    "taux_remplissage_t0",
    "capacite",
    "latitude",
    "longitude",
    "est_paris",
]
# Features numériques surveillées par le PSI (variables d'état, sensibles à un changement de régime)
DRIFT_FEATURES = ["velos_t0", "taux_remplissage_t0", "part_electriques", "tendance_1h", "moyenne_3h"]
TARGET = "velos_cible"

HOURLY_SQL = """
    SELECT h.station_id, h.heure_fin, h.velos_disponibles, h.velos_electriques, h.capacite_effective,
           h.taux_remplissage, h.minutes_depuis_releve,
           s.capacite, s.latitude, s.longitude, s.est_paris
    FROM gold.fct_station_horaire h
    JOIN gold.dim_station s USING (station_id)
    WHERE h.heure_fin >= %(since)s AND h.heure_fin <= now()
"""


def load_hourly(engine, since):
    """Heures COMPLÈTES depuis `since` (l'heure en cours, encore partielle, est exclue)."""
    df = pd.read_sql(HOURLY_SQL, engine, params={"since": since})
    df["heure_fin"] = pd.to_datetime(df["heure_fin"], utc=True)
    return df


def _shifted(df, column, hours, name):
    """Valeur de `column` décalée de `hours` heures, alignée par (station, horodatage)."""
    lagged = df[["station_id", "heure_fin", column]].copy()
    lagged["heure_fin"] = lagged["heure_fin"] + pd.Timedelta(hours=hours)
    return lagged.rename(columns={column: name})


def build_features(hourly, with_target=True):
    df = hourly.copy()
    df = df[df["minutes_depuis_releve"].fillna(np.inf) <= MAX_STALENESS_MINUTES]
    df = df.sort_values(["station_id", "heure_fin"]).reset_index(drop=True)
    df["velos_t0"] = df["velos_disponibles"].astype(float)

    for hours, name in (
        (1, "velos_lag_1h"),
        (2, "velos_lag_2h"),
        (23, "velos_cible_moins_24h"),
        (167, "velos_cible_moins_7j"),
    ):
        df = df.merge(_shifted(df, "velos_t0", hours, name), on=["station_id", "heure_fin"], how="left")
    if with_target:
        df = df.merge(_shifted(df, "velos_t0", -1, TARGET), on=["station_id", "heure_fin"], how="left")

    df["moyenne_3h"] = df[["velos_t0", "velos_lag_1h", "velos_lag_2h"]].mean(axis=1)
    df["tendance_1h"] = df["velos_t0"] - df["velos_lag_1h"]
    df["part_electriques"] = np.where(
        df["velos_t0"] > 0, df["velos_electriques"] / df["velos_t0"].replace(0, np.nan), 0.0
    )
    df["taux_remplissage_t0"] = df["taux_remplissage"].astype(float)

    target_local = (df["heure_fin"] + HORIZON).dt.tz_convert(TIMEZONE)
    fr_holidays = holidays.France(years=sorted(set(target_local.dt.year)) or [2026])
    df["heure_cible"] = target_local.dt.hour
    df["jour_semaine_cible"] = target_local.dt.dayofweek
    df["est_weekend_cible"] = (df["jour_semaine_cible"] >= 5).astype(int)
    df["est_ferie_cible"] = target_local.dt.date.map(lambda d: int(d in fr_holidays))
    df["est_paris"] = df["est_paris"].astype(int)
    df["capacite"] = df["capacite"].fillna(df["capacite_effective"]).astype(float)
    df["target_time"] = df["heure_fin"] + HORIZON

    for column in FEATURES:
        df[column] = df[column].astype(float)
    return df


def reference_bins(series, n_bins=10):
    """Bornes de déciles et proportions de référence (pour le calcul du PSI)."""
    values = series.dropna().to_numpy()
    edges = np.unique(np.quantile(values, np.linspace(0, 1, n_bins + 1)))
    if len(edges) < 2:  # variable constante
        edges = np.array([values.min() - 0.5, values.max() + 0.5]) if len(values) else np.array([0.0, 1.0])
    shares = bin_shares(values, edges)
    return edges.tolist(), shares.tolist()


def bin_shares(values, edges):
    """Proportion des valeurs dans chaque intervalle ; les extrêmes vont dans les classes de bord."""
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]
    inner = np.asarray(edges[1:-1], dtype=float)
    idx = np.searchsorted(inner, values, side="right")
    counts = np.bincount(idx, minlength=len(edges) - 1).astype(float)
    return counts / counts.sum() if counts.sum() else counts


def psi(expected_shares, actual_shares, eps=1e-4):
    """Population Stability Index = somme (a - e) * ln(a / e)."""
    e = np.clip(np.asarray(expected_shares, dtype=float), eps, None)
    a = np.clip(np.asarray(actual_shares, dtype=float), eps, None)
    return float(np.sum((a - e) * np.log(a / e)))
