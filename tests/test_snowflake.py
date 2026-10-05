"""Tests for Snowflake data warehouse integration."""

import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")


def test_snowflake_connector_module_exists():
    """Verify Snowflake connector module exists."""
    assert os.path.exists(os.path.join(ROOT, "snowflake", "snowflake_connector.py"))


def test_snowflake_init_sql_exists():
    """Verify Snowflake warehouse SQL exists."""
    assert os.path.exists(os.path.join(ROOT, "snowflake", "init_warehouse.sql"))


def test_snowflake_sql_has_warehouse():
    """Verify Snowflake SQL creates warehouse infrastructure."""
    with open(os.path.join(ROOT, "snowflake", "init_warehouse.sql")) as f:
        sql = f.read()

    assert "CREATE WAREHOUSE" in sql
    assert "PIPELINE_WH" in sql
    assert "CREATE DATABASE" in sql
    assert "PIPELINE_DB" in sql
    assert "ANALYTICS" in sql
    assert "STAGING" in sql


def test_snowflake_sql_has_dimensions():
    """Verify Snowflake SQL creates dimension tables."""
    with open(os.path.join(ROOT, "snowflake", "init_warehouse.sql")) as f:
        sql = f.read()

    assert "DIM_CUSTOMERS" in sql
    assert "DIM_DATE" in sql
    assert "DIM_PRODUCTS" in sql
    assert "DIM_DEVICES" in sql


def test_snowflake_sql_has_facts():
    """Verify Snowflake SQL creates fact tables."""
    with open(os.path.join(ROOT, "snowflake", "init_warehouse.sql")) as f:
        sql = f.read()

    assert "FACT_ORDERS" in sql
    assert "FACT_SENSOR_READINGS" in sql
    assert "FACT_PIPELINE_RUNS" in sql


def test_snowflake_sql_has_staging():
    """Verify Snowflake SQL creates staging tables."""
    with open(os.path.join(ROOT, "snowflake", "init_warehouse.sql")) as f:
        sql = f.read()

    assert "STG_ORDERS" in sql
    assert "STG_SENSOR_READINGS" in sql
    assert "PIPELINE_STAGE" in sql


def test_snowflake_sql_has_clustering():
    """Verify Snowflake SQL uses clustering keys for performance."""
    with open(os.path.join(ROOT, "snowflake", "init_warehouse.sql")) as f:
        sql = f.read()

    assert "CLUSTER BY" in sql


def test_snowflake_sql_has_tasks():
    """Verify Snowflake SQL creates automated tasks."""
    with open(os.path.join(ROOT, "snowflake", "init_warehouse.sql")) as f:
        sql = f.read()

    assert "CREATE OR REPLACE TASK" in sql
    assert "REFRESH_DAILY_ORDERS_AGG" in sql
    assert "MERGE INTO" in sql


def test_snowflake_sql_has_grants():
    """Verify Snowflake SQL includes proper RBAC grants."""
    with open(os.path.join(ROOT, "snowflake", "init_warehouse.sql")) as f:
        sql = f.read()

    assert "GRANT USAGE" in sql
    assert "PIPELINE_ROLE" in sql


def test_warehouse_dag_references_snowflake():
    """Verify warehouse DAG integrates with Snowflake."""
    dag_path = os.path.join(ROOT, "airflow", "dags", "warehouse_transform_dag.py")
    with open(dag_path) as f:
        content = f.read()

    assert "SNOWFLAKE_ENABLED" in content
    assert "snowflake" in content.lower()
    assert "STAGING.STG_ORDERS" in content
    assert "ANALYTICS.DIM_CUSTOMERS" in content
    assert "ANALYTICS.FACT_ORDERS" in content


def test_bi_dashboard_supports_snowflake():
    """Verify BI dashboard queries Snowflake."""
    with open(os.path.join(ROOT, "bi_dashboards", "bi_dashboard.py")) as f:
        content = f.read()

    assert "SNOWFLAKE_ENABLED" in content
    assert "get_snowflake_engine" in content
    assert "SNOWFLAKE_QUERIES" in content
    assert "ANALYTICS.DIM_DATE" in content


def test_dotnet_warehouse_controller_has_snowflake():
    """Verify .NET warehouse controller references Snowflake."""
    path = os.path.join(
        ROOT, "sample_dotnet_backend", "src", "DataPipelineApi", "Controllers", "WarehouseController.cs"
    )
    with open(path) as f:
        content = f.read()

    assert "snowflake" in content.lower()
    assert "IsSnowflakeConfigured" in content
    assert "PIPELINE_DB" in content


def test_env_has_snowflake_config():
    """Verify environment config has Snowflake settings."""
    # Use .env.example since .env is gitignored and won't exist in CI
    env_file = os.path.join(ROOT, ".env")
    if not os.path.exists(env_file):
        env_file = os.path.join(ROOT, ".env.example")
    with open(env_file) as f:
        content = f.read()

    assert "SNOWFLAKE_ACCOUNT" in content
    assert "SNOWFLAKE_USER" in content
    assert "SNOWFLAKE_PASSWORD" in content
    assert "SNOWFLAKE_WAREHOUSE" in content
    assert "SNOWFLAKE_DATABASE" in content
    assert "SNOWFLAKE_SCHEMA" in content
    assert "SNOWFLAKE_ROLE" in content


def test_connector_has_required_functions():
    """Verify Snowflake connector module has all required functions."""
    with open(os.path.join(ROOT, "snowflake", "snowflake_connector.py")) as f:
        content = f.read()

    assert "def get_connection" in content
    assert "def execute_sql" in content
    assert "def query_to_dataframe" in content
    assert "def load_dataframe" in content
    assert "def stage_and_copy" in content
    assert "def check_connection" in content
