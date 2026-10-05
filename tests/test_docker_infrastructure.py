"""Tests for Docker infrastructure configuration."""

import os
import re

import yaml

ROOT = os.path.join(os.path.dirname(__file__), "..")


def load_compose():
    """Load and parse docker-compose.yaml."""
    with open(os.path.join(ROOT, "docker-compose.yaml")) as f:
        return yaml.safe_load(f)


def test_compose_services_count():
    """Verify expected number of services are defined."""
    compose = load_compose()
    services = list(compose.get("services", {}).keys())
    assert len(services) >= 20, f"Expected at least 20 services, got {len(services)}: {services}"


def test_all_services_have_networks():
    """Verify all services are attached to pipeline-network."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        if name in ("minio-init", "airflow-init"):
            continue  # init containers may not need persistent network
        networks = svc.get("networks", [])
        assert "pipeline-network" in networks, f"Service '{name}' missing pipeline-network"


def test_persistent_services_have_resource_limits():
    """Verify services with restart policy have resource limits."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        if svc.get("restart") == "unless-stopped":
            deploy = svc.get("deploy", {})
            resources = deploy.get("resources", {})
            limits = resources.get("limits", {})
            assert "memory" in limits, f"Service '{name}' missing memory limit"


def test_persistent_services_have_logging():
    """Verify long-running services have logging configuration."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        if svc.get("restart") == "unless-stopped":
            logging = svc.get("logging", {})
            assert logging.get("driver") == "json-file", f"Service '{name}' missing json-file logging driver"


def test_no_latest_tags_on_persistent_services():
    """Verify no :latest tags on long-running service images."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        if svc.get("restart") != "unless-stopped":
            continue
        image = svc.get("image", "")
        if image:
            assert ":latest" not in image, f"Service '{name}' uses :latest tag: {image}"


def test_volumes_defined():
    """Verify all named volumes are declared."""
    compose = load_compose()
    volumes = list(compose.get("volumes", {}).keys())
    expected = [
        "mysql_data",
        "postgres_data",
        "redis_data",
        "mongodb_data",
        "minio_data",
        "prometheus_data",
        "grafana_data",
        "airflow_logs",
        "elasticsearch_data",
        "spark_checkpoints",
    ]
    for vol in expected:
        assert vol in volumes, f"Volume '{vol}' not declared"


def test_env_files_match():
    """Verify .env and .env.example have the same keys (skip if .env not present, e.g. CI)."""
    env_path = os.path.join(ROOT, ".env")
    example_path = os.path.join(ROOT, ".env.example")

    assert os.path.exists(example_path), ".env.example must exist"

    # In CI, .env is gitignored and won't exist — that's fine, just verify .env.example is valid
    if not os.path.exists(env_path):
        return

    def parse_keys(path):
        keys = set()
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    keys.add(line.split("=", 1)[0])
        return keys

    env_keys = parse_keys(env_path)
    example_keys = parse_keys(example_path)
    assert (
        env_keys == example_keys
    ), f"Key mismatch: in .env only: {env_keys - example_keys}, in .env.example only: {example_keys - env_keys}"


def test_dockerfiles_exist_for_build_services():
    """Verify Dockerfile exists for all services that use build."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        build = svc.get("build")
        if build:
            context = build if isinstance(build, str) else build.get("context", ".")
            dockerfile = os.path.join(ROOT, context, "Dockerfile")
            assert os.path.exists(
                dockerfile
            ), f"Service '{name}' references build context '{context}' but Dockerfile not found"


def test_persistent_services_have_healthchecks():
    """Verify all long-running services have healthchecks."""
    compose = load_compose()
    exempt = {"minio-init", "airflow-init"}
    for name, svc in compose["services"].items():
        if name in exempt:
            continue
        if svc.get("restart") == "unless-stopped":
            assert "healthcheck" in svc, f"Service '{name}' is long-running but has no healthcheck"


def test_all_services_have_labels():
    """Verify all services have component and tier labels."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        labels = svc.get("labels", {})
        assert "com.pipeline.component" in labels, f"Service '{name}' missing com.pipeline.component label"
        assert "com.pipeline.tier" in labels, f"Service '{name}' missing com.pipeline.tier label"


def test_mlflow_and_influxdb_services_exist():
    """Verify MLflow and InfluxDB services are defined."""
    compose = load_compose()
    services = list(compose["services"].keys())
    assert "mlflow" in services, "MLflow service not defined"
    assert "influxdb" in services, "InfluxDB service not defined"
