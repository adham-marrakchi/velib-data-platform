"""Référentiel des stations Vélib' : station_information.json -> MinIO (bronze) -> staging.velib_stations."""

import json
import logging
import time
from datetime import datetime, timezone

import requests

from common.config import GBFS_BASE_URL, LAKE_BUCKET, MINIO_ENDPOINT

logger = logging.getLogger(__name__)
GEO_API_URL = "https://geo.api.gouv.fr/communes"


def s3_client():
    import os

    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        region_name="us-east-1",
    )


def fetch_to_bronze(logical_date=None):
    """Télécharge station_information.json et le stocke tel quel dans le lake. Renvoie la clé S3."""
    response = requests.get(f"{GBFS_BASE_URL}/station_information.json", timeout=(5, 30))
    response.raise_for_status()
    payload = response.json()
    stations = payload.get("data", {}).get("stations")
    if not stations:
        raise ValueError("station_information.json ne contient aucune station")

    now = logical_date or datetime.now(timezone.utc)
    key = f"bronze/velib_station_information/snapshot_date={now:%Y-%m-%d}/station_information_{now:%H%M%S}.json"
    s3_client().put_object(Bucket=LAKE_BUCKET, Key=key, Body=response.content, ContentType="application/json")
    logger.info("%d stations archivées dans s3://%s/%s", len(stations), LAKE_BUCKET, key)
    return key


def parse_stations(payload):
    """Extrait les champs utiles. Ignore les stations sans identifiant ou coordonnées."""
    rows = []
    for s in payload.get("data", {}).get("stations", []):
        if s.get("station_id") is None or s.get("lat") is None or s.get("lon") is None:
            continue
        rows.append(
            {
                "station_id": int(s["station_id"]),
                "station_code": s.get("stationCode"),
                "station_name": (s.get("name") or "").strip() or f"Station {s['station_id']}",
                "lat": float(s["lat"]),
                "lon": float(s["lon"]),
                "capacity": s.get("capacity"),
            }
        )
    return rows


def reverse_geocode(lat, lon, session=None):
    """Commune (ou arrondissement pour Paris) via l'API Géo de l'État. Renvoie (nom, code_insee)."""
    session = session or requests
    for params in (
        {"type": "arrondissement-municipal"},  # Paris -> "Paris 16e Arrondissement"
        {},  # autres communes de la métropole
    ):
        response = session.get(
            GEO_API_URL, params={"lat": lat, "lon": lon, "fields": "nom,code", **params}, timeout=(5, 15)
        )
        response.raise_for_status()
        results = response.json()
        if results:
            return results[0]["nom"], results[0]["code"]
    return None, None


def upsert_stations(s3_key):
    """Charge le snapshot dans staging.velib_stations ; géocode uniquement les nouvelles stations."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from common.config import WAREHOUSE_CONN_ID

    body = s3_client().get_object(Bucket=LAKE_BUCKET, Key=s3_key)["Body"].read()
    rows = parse_stations(json.loads(body))
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)

    known = {r[0]: r[1] for r in hook.get_records("SELECT station_id, code_insee FROM staging.velib_stations")}
    session = requests.Session()
    geocoded = 0
    for row in rows:
        if known.get(row["station_id"]):
            row["commune"], row["code_insee"] = None, None  # déjà connue : on conserve la valeur en base
            continue
        try:
            row["commune"], row["code_insee"] = reverse_geocode(row["lat"], row["lon"], session)
            geocoded += 1
            time.sleep(0.05)  # l'API Géo limite à 50 requêtes/s
        except requests.RequestException as exc:
            logger.warning("Géocodage impossible pour %s : %s", row["station_id"], exc)
            row["commune"], row["code_insee"] = None, None

    sql = """
        INSERT INTO staging.velib_stations
            (station_id, station_code, station_name, lat, lon, capacity, commune, code_insee)
        VALUES (%(station_id)s, %(station_code)s, %(station_name)s, %(lat)s, %(lon)s, %(capacity)s,
                %(commune)s, %(code_insee)s)
        ON CONFLICT (station_id) DO UPDATE SET
            station_code = EXCLUDED.station_code,
            station_name = EXCLUDED.station_name,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            capacity = EXCLUDED.capacity,
            commune = COALESCE(EXCLUDED.commune, staging.velib_stations.commune),
            code_insee = COALESCE(EXCLUDED.code_insee, staging.velib_stations.code_insee),
            last_seen = current_date,
            updated_at = now()
    """
    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
    logger.info("%d stations chargées (%d géocodées).", len(rows), geocoded)
    return {"stations": len(rows), "geocoded": geocoded}
