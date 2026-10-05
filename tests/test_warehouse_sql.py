"""Tests for data warehouse SQL schema validity."""

import os

ROOT = os.path.join(os.path.dirname(__file__), "..")


def test_warehouse_sql_has_dim_tables():
    """Verify warehouse SQL contains all dimension tables."""
    sql_path = os.path.join(ROOT, "scripts", "init_warehouse.sql")
    with open(sql_path) as f:
        sql = f.read()

    assert "dim_customers" in sql
    assert "dim_date" in sql
    assert "dim_products" in sql
    assert "dim_devices" in sql


def test_warehouse_sql_has_fact_tables():
    """Verify warehouse SQL contains all fact tables."""
    sql_path = os.path.join(ROOT, "scripts", "init_warehouse.sql")
    with open(sql_path) as f:
        sql = f.read()

    assert "fact_orders" in sql
    assert "fact_sensor_readings" in sql
    assert "fact_pipeline_runs" in sql


def test_warehouse_sql_has_aggregation_tables():
    """Verify warehouse SQL contains aggregation tables."""
    sql_path = os.path.join(ROOT, "scripts", "init_warehouse.sql")
    with open(sql_path) as f:
        sql = f.read()

    assert "agg_daily_orders" in sql
    assert "agg_hourly_sensors" in sql


def test_warehouse_sql_has_indexes():
    """Verify warehouse SQL contains performance indexes."""
    sql_path = os.path.join(ROOT, "scripts", "init_warehouse.sql")
    with open(sql_path) as f:
        sql = f.read()

    assert "CREATE INDEX" in sql
    assert "idx_fact_orders_customer" in sql
    assert "idx_fact_sensors_device" in sql
    assert "idx_fact_sensors_anomaly" in sql


def test_warehouse_sql_populates_dim_date():
    """Verify warehouse SQL populates dim_date dimension."""
    sql_path = os.path.join(ROOT, "scripts", "init_warehouse.sql")
    with open(sql_path) as f:
        sql = f.read()

    assert "generate_series" in sql
    assert "2024-01-01" in sql
    assert "2026-12-31" in sql


def test_source_sql_has_tables():
    """Verify MySQL init SQL has required tables."""
    sql_path = os.path.join(ROOT, "scripts", "init_db.sql")
    with open(sql_path) as f:
        sql = f.read()

    assert "orders" in sql
    assert "customers" in sql


def test_warehouse_dag_exists():
    """Verify warehouse transformation DAG exists."""
    dag_path = os.path.join(ROOT, "airflow", "dags", "warehouse_transform_dag.py")
    assert os.path.exists(dag_path)

    with open(dag_path) as f:
        content = f.read()

    assert "warehouse_transform_dag" in content
    assert "load_dimensions" in content
    assert "load_facts" in content
    assert "refresh_aggregations" in content
    assert "execution_timeout" in content
    assert "SNOWFLAKE_ENABLED" in content


def test_dotnet_warehouse_controller_exists():
    """Verify .NET warehouse controller exists."""
    path = os.path.join(
        ROOT, "sample_dotnet_backend", "src", "DataPipelineApi", "Controllers", "WarehouseController.cs"
    )
    assert os.path.exists(path)

    with open(path) as f:
        content = f.read()

    assert "api/warehouse" in content
    assert "transform" in content
