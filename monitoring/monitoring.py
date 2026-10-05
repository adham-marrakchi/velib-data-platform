import json
import logging
import os
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROMETHEUS_PORT = os.getenv("PROMETHEUS_PORT", "9090")
GRAFANA_PORT = os.getenv("GRAFANA_PORT", "3000")
GRAFANA_API_URL = f"http://grafana:{GRAFANA_PORT}/api"
GRAFANA_ADMIN_USER = os.getenv("GRAFANA_ADMIN_USER", "admin")
GRAFANA_ADMIN_PASS = os.getenv("GRAFANA_ADMIN_PASS", "admin_secret_2024")
DASHBOARDS_PATH = os.getenv("DASHBOARDS_PATH", "/opt/dashboards")


def wait_for_grafana(timeout=60):
    """Wait until Grafana API is responsive."""
    logger.info("Waiting for Grafana to be ready...")
    for _ in range(timeout):
        try:
            response = requests.get(f"{GRAFANA_API_URL}/health", timeout=5)
            if response.status_code == 200:
                logger.info("Grafana is ready.")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(1)
    logger.error("Grafana did not start within %ds.", timeout)
    return False


def create_grafana_datasource():
    """Create Prometheus datasource in Grafana."""
    logger.info("Creating Prometheus datasource in Grafana...")
    payload = {
        "name": "Prometheus",
        "type": "prometheus",
        "url": f"http://prometheus:{PROMETHEUS_PORT}",
        "access": "proxy",
        "isDefault": True,
    }

    response = requests.post(
        f"{GRAFANA_API_URL}/datasources",
        auth=(GRAFANA_ADMIN_USER, GRAFANA_ADMIN_PASS),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=10,
    )

    if response.status_code in (200, 201):
        logger.info("Prometheus datasource created.")
    elif response.status_code == 409:
        logger.info("Prometheus datasource already exists.")
    else:
        logger.error("Failed to create datasource: %s", response.text)


def import_grafana_dashboards():
    """Import dashboard JSON files into Grafana."""
    if not os.path.exists(DASHBOARDS_PATH):
        logger.warning("Dashboard directory not found: %s", DASHBOARDS_PATH)
        return

    for filename in os.listdir(DASHBOARDS_PATH):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(DASHBOARDS_PATH, filename)
        with open(filepath) as f:
            dashboard_data = json.load(f)

        payload = {"dashboard": dashboard_data, "overwrite": True}
        response = requests.post(
            f"{GRAFANA_API_URL}/dashboards/db",
            auth=(GRAFANA_ADMIN_USER, GRAFANA_ADMIN_PASS),
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=10,
        )

        if response.status_code in (200, 201):
            logger.info("Imported dashboard: %s", filename)
        else:
            logger.error("Failed to import %s: %s", filename, response.text)


def main():
    """Initialize monitoring: configure Grafana datasource and dashboards."""
    if wait_for_grafana():
        create_grafana_datasource()
        import_grafana_dashboards()
    logger.info("Monitoring setup complete.")


if __name__ == "__main__":
    main()
