"""Alertes de la plateforme.

Chaque alerte est :
  1. journalisée dans monitoring.alerts (visible dans Grafana, même sans canal externe) ;
  2. envoyée aux canaux configurés : webhook générique (n8n), Slack, Telegram.
Un anti-spam évite de renvoyer la même alerte plus d'une fois par fenêtre (par défaut 60 min).
"""

import logging
import os
from functools import partial

import requests

logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SEVERITY_EMOJI = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}


class AlertedFailure(Exception):
    """Échec dont l'alerte spécifique a déjà été envoyée (le callback générique l'ignore)."""


def _send_channels(alert_type, severity, title, message):
    delivered = []
    text = f"{SEVERITY_EMOJI.get(severity, '')} [{severity.upper()}] {title}\n{message or ''}".strip()
    payload = {"alert_type": alert_type, "severity": severity, "title": title, "message": message}
    channels = [
        ("webhook", WEBHOOK_URL, lambda: requests.post(WEBHOOK_URL, json=payload, timeout=10)),
        ("slack", SLACK_WEBHOOK_URL, lambda: requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)),
        (
            "telegram",
            TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID,
            lambda: requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                timeout=10,
            ),
        ),
    ]
    for name, enabled, send in channels:
        if not enabled:
            continue
        try:
            send().raise_for_status()
            delivered.append(name)
        except requests.RequestException as exc:
            logger.error("Envoi de l'alerte via %s impossible : %s", name, exc)
    return delivered


def notify(alert_type, severity, title, message="", source=None, dedup_minutes=60):
    """Enregistre et diffuse une alerte. Renvoie False si elle a été dédoublonnée."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from common.config import WAREHOUSE_CONN_ID

    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    if dedup_minutes:
        recent = hook.get_first(
            "SELECT 1 FROM monitoring.alerts WHERE alert_type = %s AND title = %s "
            "AND created_at > now() - make_interval(mins => %s)",
            parameters=(alert_type, title, dedup_minutes),
        )
        if recent:
            logger.info("Alerte '%s' déjà émise il y a moins de %d min : ignorée.", title, dedup_minutes)
            return False

    delivered = _send_channels(alert_type, severity, title, message)
    hook.run(
        "INSERT INTO monitoring.alerts (alert_type, severity, title, message, source, delivered_to) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        parameters=(alert_type, severity, title, message, source, delivered or ["database"]),
    )
    logger.warning("ALERTE [%s/%s] %s — canaux : %s", alert_type, severity, title, delivered or ["database"])
    return True


def _on_failure(context, alert_type="dag_failure", severity="critical"):
    exception = context.get("exception")
    if isinstance(exception, AlertedFailure):
        return
    ti = context["ti"]
    from common.config import AIRFLOW_BASE_URL

    url = f"{AIRFLOW_BASE_URL}/dags/{ti.dag_id}/runs/{ti.run_id}/tasks/{ti.task_id}"
    try:
        notify(
            alert_type,
            severity,
            title=f"Échec de {ti.dag_id}.{ti.task_id}",
            message=f"Run {ti.run_id} — {exception}\n{url}",
            source=ti.dag_id,
            dedup_minutes=30,
        )
    except Exception:  # une alerte ne doit jamais masquer l'erreur d'origine
        logger.exception("Impossible d'émettre l'alerte d'échec")


on_failure_callback = _on_failure
on_quality_failure = partial(_on_failure, alert_type="data_quality")
