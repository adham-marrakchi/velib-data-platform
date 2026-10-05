"""Producteur Kafka Vélib' : interroge le flux GBFS station_status et publie chaque station dans Kafka.

Choix de conception :
- Le message publié est l'objet station BRUT (tel que renvoyé par l'API) enveloppé de métadonnées
  d'ingestion. Aucune transformation métier ici : le typage et le nettoyage sont faits en silver.
- Clé Kafka = station_id : tous les relevés d'une station vont dans la même partition, ce qui
  garantit leur ordre.
- Option VELIB_ONLY_CHANGED : on ne republie une station que si son last_reported a changé depuis
  le dernier appel (capture de changement). Le volume baisse fortement sans perte d'information,
  car un état non modifié se reconstruit en aval par "forward fill".
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("velib-producer")

GBFS_BASE_URL = os.getenv(
    "VELIB_GBFS_BASE_URL", "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
)
STATUS_URL = f"{GBFS_BASE_URL}/station_status.json"
POLL_INTERVAL = float(os.getenv("VELIB_POLL_INTERVAL_SECONDS", "60"))
HTTP_TIMEOUT = float(os.getenv("VELIB_HTTP_TIMEOUT_SECONDS", "20"))
ONLY_CHANGED = os.getenv("VELIB_ONLY_CHANGED", "true").lower() == "true"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "velib_status")
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))
TOPIC_RETENTION_MS = int(os.getenv("KAFKA_TOPIC_RETENTION_MS", str(3 * 24 * 3600 * 1000)))
METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", "8000"))


class FeedError(Exception):
    """Réponse GBFS inexploitable (JSON invalide ou structure inattendue)."""


# ---------------------------------------------------------------------------
# Fonctions pures (testées unitairement)
# ---------------------------------------------------------------------------
def parse_status_payload(payload):
    """Valide la structure GBFS et renvoie (last_updated, liste des stations)."""
    if not isinstance(payload, dict):
        raise FeedError("Le flux GBFS n'est pas un objet JSON")
    stations = payload.get("data", {}).get("stations")
    if not isinstance(stations, list):
        raise FeedError("Champ data.stations absent ou invalide")
    last_updated = payload.get("lastUpdatedOther") or payload.get("last_updated")
    return last_updated, stations


def build_messages(stations, feed_last_updated, fetched_at, last_seen, only_changed=True):
    """Construit les messages (clé, valeur) à publier.

    last_seen : dict station_id -> last_reported déjà publié. Renvoie (messages, last_seen mis à jour).
    Les stations sans station_id sont ignorées (impossible de les clé-er).
    """
    messages = []
    updated = dict(last_seen)
    for station in stations:
        station_id = station.get("station_id")
        if station_id is None:
            continue
        last_reported = station.get("last_reported")
        key = str(station_id)
        if only_changed and updated.get(key) == last_reported:
            continue
        updated[key] = last_reported
        value = {
            "feed_last_updated": feed_last_updated,
            "fetched_at": fetched_at,
            "source": "velib_gbfs_station_status",
            "station": station,
        }
        messages.append((key, value))
    return messages, updated


def serialize(value):
    """Sérialise un message en JSON UTF-8 compact."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------
# Entrées / sorties
# ---------------------------------------------------------------------------
def create_http_session():
    """Session HTTP avec retries exponentiels sur erreurs réseau, 429 et 5xx."""
    retry = Retry(
        total=5,
        connect=5,
        read=3,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "velib-data-platform/1.0 (projet personnel open data)"
    return session


def fetch_status(session, url=STATUS_URL, timeout=HTTP_TIMEOUT):
    """Télécharge station_status.json. Lève requests.RequestException ou FeedError."""
    response = session.get(url, timeout=(5, timeout))
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise FeedError(f"JSON invalide : {exc}") from exc
    return parse_status_payload(payload)


def ensure_topic(admin_client, topic=KAFKA_TOPIC):
    """Crée le topic s'il n'existe pas (3 partitions, rétention 3 jours)."""
    from confluent_kafka.admin import NewTopic

    metadata = admin_client.list_topics(timeout=10)
    if topic in metadata.topics:
        logger.info("Topic '%s' déjà présent.", topic)
        return
    new_topic = NewTopic(
        topic,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=1,
        config={"retention.ms": str(TOPIC_RETENTION_MS)},
    )
    for name, future in admin_client.create_topics([new_topic]).items():
        try:
            future.result()
            logger.info("Topic '%s' créé.", name)
        except Exception as exc:  # TopicExistsError si création concurrente
            logger.warning("Création du topic '%s' : %s", name, exc)


def create_producer():
    """Producteur idempotent : acks=all + enable.idempotence évitent doublons et pertes côté broker."""
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "client.id": "velib-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "gzip",
            "linger.ms": 50,
            "message.timeout.ms": 120000,
        }
    )


class Metrics:
    """Métriques Prometheus exposées sur :8000/metrics."""

    def __init__(self):
        from prometheus_client import Counter, Gauge, Histogram

        self.messages = Counter("velib_producer_messages_total", "Messages publiés dans Kafka")
        self.delivery_errors = Counter("velib_producer_delivery_errors_total", "Échecs de livraison Kafka")
        self.fetch_errors = Counter("velib_producer_fetch_errors_total", "Appels GBFS en échec", ["reason"])
        self.fetch_duration = Histogram("velib_producer_fetch_seconds", "Durée d'un appel GBFS")
        self.last_success = Gauge("velib_producer_last_success_timestamp", "Horodatage du dernier cycle réussi")
        self.stations = Gauge("velib_producer_stations_in_feed", "Stations présentes dans le flux")
        self.changed = Gauge("velib_producer_stations_changed", "Stations publiées au dernier cycle")


def run():
    from confluent_kafka.admin import AdminClient
    from prometheus_client import start_http_server

    stop = {"requested": False}

    def _handle_stop(signum, _frame):
        logger.info("Signal %s reçu, arrêt propre en cours...", signum)
        stop["requested"] = True

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    metrics = Metrics()
    start_http_server(METRICS_PORT)

    # Attente du broker (au démarrage de la stack Kafka peut ne pas être prêt)
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    for attempt in range(1, 31):
        try:
            ensure_topic(admin)
            break
        except Exception as exc:
            logger.warning("Kafka indisponible (tentative %d/30) : %s", attempt, exc)
            time.sleep(5)
    else:
        raise SystemExit("Kafka injoignable après 30 tentatives")

    producer = create_producer()
    session = create_http_session()
    last_seen = {}

    def on_delivery(err, _msg):
        if err is not None:
            metrics.delivery_errors.inc()
            logger.error("Échec de livraison : %s", err)
        else:
            metrics.messages.inc()

    logger.info("Collecte de %s toutes les %ss vers le topic '%s'", STATUS_URL, POLL_INTERVAL, KAFKA_TOPIC)
    while not stop["requested"]:
        cycle_start = time.monotonic()
        try:
            with metrics.fetch_duration.time():
                feed_last_updated, stations = fetch_status(session)
            fetched_at = datetime.now(timezone.utc).isoformat()
            messages, last_seen = build_messages(stations, feed_last_updated, fetched_at, last_seen, ONLY_CHANGED)
            for key, value in messages:
                producer.produce(KAFKA_TOPIC, key=key.encode(), value=serialize(value), on_delivery=on_delivery)
                producer.poll(0)
            remaining = producer.flush(60)
            if remaining:
                logger.error("%d messages non confirmés après flush", remaining)
            metrics.stations.set(len(stations))
            metrics.changed.set(len(messages))
            metrics.last_success.set_to_current_time()
            logger.info("%d stations dans le flux, %d publiées.", len(stations), len(messages))
        except requests.RequestException as exc:
            metrics.fetch_errors.labels(reason="http").inc()
            logger.error("Erreur réseau GBFS (après retries) : %s", exc)
        except FeedError as exc:
            metrics.fetch_errors.labels(reason="payload").inc()
            logger.error("Flux GBFS invalide : %s", exc)

        # Attente fractionnée pour réagir vite à SIGTERM
        while not stop["requested"] and time.monotonic() - cycle_start < POLL_INTERVAL:
            time.sleep(0.5)

    producer.flush(30)
    logger.info("Producteur arrêté.")


if __name__ == "__main__":
    run()
