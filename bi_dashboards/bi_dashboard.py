"""
BI Dashboard Integration - Export warehouse data to BI platforms.

Queries the Snowflake data warehouse (or PostgreSQL fallback) and uploads
aggregated data to Tableau, Looker, and Power BI for business analytics.
"""

import logging
import os
import sys

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Snowflake Configuration
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_ENABLED = bool(SNOWFLAKE_ACCOUNT)

# PostgreSQL fallback
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "processed_db")
DB_USER = os.getenv("POSTGRES_USER", "pipeline_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_secret_2024")

# BI Tool configs
TABLEAU_SERVER = os.getenv("TABLEAU_SERVER", "")
LOOKER_API_URL = os.getenv("LOOKER_API_URL", "")
POWER_BI_WORKSPACE_ID = os.getenv("POWER_BI_WORKSPACE_ID", "")

OUTPUT_DIR = os.getenv("BI_OUTPUT_DIR", "/tmp/bi_exports")

# Snowflake-compatible warehouse queries
SNOWFLAKE_QUERIES = {
    "daily_orders_summary": """
        SELECT
            dd.FULL_DATE,
            dd.DAY_NAME,
            dd.MONTH_NAME,
            dd.QUARTER,
            dd.YEAR,
            COALESCE(a.TOTAL_ORDERS, 0) AS TOTAL_ORDERS,
            COALESCE(a.TOTAL_REVENUE, 0) AS TOTAL_REVENUE,
            COALESCE(a.AVG_ORDER_VALUE, 0) AS AVG_ORDER_VALUE,
            COALESCE(a.UNIQUE_CUSTOMERS, 0) AS UNIQUE_CUSTOMERS
        FROM ANALYTICS.DIM_DATE dd
        LEFT JOIN ANALYTICS.AGG_DAILY_ORDERS a ON a.DATE_KEY = dd.DATE_KEY
        WHERE dd.YEAR >= 2024
        ORDER BY dd.FULL_DATE
    """,
    "customer_segments": """
        SELECT
            dc.CUSTOMER_ID,
            dc.CUSTOMER_NAME,
            dc.CUSTOMER_SEGMENT,
            COUNT(fo.ORDER_KEY) AS TOTAL_ORDERS,
            SUM(fo.NET_AMOUNT) AS TOTAL_SPENT,
            AVG(fo.NET_AMOUNT) AS AVG_ORDER_VALUE,
            MAX(dd.FULL_DATE) AS LAST_ORDER_DATE
        FROM ANALYTICS.DIM_CUSTOMERS dc
        LEFT JOIN ANALYTICS.FACT_ORDERS fo ON fo.CUSTOMER_KEY = dc.CUSTOMER_KEY
        LEFT JOIN ANALYTICS.DIM_DATE dd ON dd.DATE_KEY = fo.DATE_KEY
        WHERE dc.IS_CURRENT = TRUE
        GROUP BY dc.CUSTOMER_ID, dc.CUSTOMER_NAME, dc.CUSTOMER_SEGMENT
        ORDER BY TOTAL_SPENT DESC
    """,
    "sensor_anomaly_report": """
        SELECT
            dd.DEVICE_ID,
            dd.DEVICE_TYPE,
            dd.LOCATION,
            COUNT(fsr.READING_KEY) AS TOTAL_ANOMALIES,
            AVG(fsr.READING_VALUE) AS AVG_ANOMALY_VALUE,
            MAX(fsr.READING_VALUE) AS MAX_ANOMALY_VALUE,
            MAX(fsr.READING_TIMESTAMP) AS LAST_ANOMALY
        FROM ANALYTICS.DIM_DEVICES dd
        JOIN ANALYTICS.FACT_SENSOR_READINGS fsr ON fsr.DEVICE_KEY = dd.DEVICE_KEY
        WHERE fsr.IS_ANOMALY = TRUE
        GROUP BY dd.DEVICE_ID, dd.DEVICE_TYPE, dd.LOCATION
        ORDER BY TOTAL_ANOMALIES DESC
    """,
    "pipeline_health": """
        SELECT
            DAG_ID,
            RUN_TYPE,
            COUNT(*) AS TOTAL_RUNS,
            SUM(CASE WHEN STATUS = 'success' THEN 1 ELSE 0 END) AS SUCCESSFUL_RUNS,
            SUM(CASE WHEN STATUS = 'failed' THEN 1 ELSE 0 END) AS FAILED_RUNS,
            AVG(DURATION_SECONDS) AS AVG_DURATION_SECONDS,
            SUM(RECORDS_PROCESSED) AS TOTAL_RECORDS_PROCESSED
        FROM ANALYTICS.FACT_PIPELINE_RUNS
        WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
        GROUP BY DAG_ID, RUN_TYPE
        ORDER BY DAG_ID
    """,
}

# PostgreSQL fallback queries (lowercase)
PG_QUERIES = {
    "daily_orders_summary": """
        SELECT dd.full_date, dd.day_name, dd.month_name, dd.quarter, dd.year,
               COALESCE(a.total_orders, 0) AS total_orders,
               COALESCE(a.total_revenue, 0) AS total_revenue,
               COALESCE(a.avg_order_value, 0) AS avg_order_value,
               COALESCE(a.unique_customers, 0) AS unique_customers
        FROM dim_date dd
        LEFT JOIN agg_daily_orders a ON a.date_key = dd.date_key
        WHERE dd.year >= 2024 ORDER BY dd.full_date
    """,
    "customer_segments": """
        SELECT dc.customer_id, dc.customer_name, dc.customer_segment,
               COUNT(fo.order_key) AS total_orders, SUM(fo.net_amount) AS total_spent,
               AVG(fo.net_amount) AS avg_order_value, MAX(dd.full_date) AS last_order_date
        FROM dim_customers dc
        LEFT JOIN fact_orders fo ON fo.customer_key = dc.customer_key
        LEFT JOIN dim_date dd ON dd.date_key = fo.date_key
        WHERE dc.is_current = TRUE
        GROUP BY dc.customer_id, dc.customer_name, dc.customer_segment
        ORDER BY total_spent DESC
    """,
    "sensor_anomaly_report": """
        SELECT dd.device_id, dd.device_type, dd.location,
               COUNT(fsr.reading_key) AS total_anomalies,
               AVG(fsr.reading_value) AS avg_anomaly_value,
               MAX(fsr.reading_value) AS max_anomaly_value,
               MAX(fsr.reading_timestamp) AS last_anomaly
        FROM dim_devices dd
        JOIN fact_sensor_readings fsr ON fsr.device_key = dd.device_key
        WHERE fsr.is_anomaly = TRUE
        GROUP BY dd.device_id, dd.device_type, dd.location
        ORDER BY total_anomalies DESC
    """,
    "pipeline_health": """
        SELECT dag_id, run_type, COUNT(*) AS total_runs,
               SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
               AVG(duration_seconds) AS avg_duration_seconds,
               SUM(records_processed) AS total_records_processed
        FROM fact_pipeline_runs
        WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '30 days'
        GROUP BY dag_id, run_type ORDER BY dag_id
    """,
}


def get_snowflake_engine():
    """Create SQLAlchemy engine for Snowflake."""
    from sqlalchemy import create_engine

    url = (
        f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}"
        f"@{SNOWFLAKE_ACCOUNT}/{os.getenv('SNOWFLAKE_DATABASE', 'PIPELINE_DB')}"
        f"/{os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS')}"
        f"?warehouse={os.getenv('SNOWFLAKE_WAREHOUSE', 'PIPELINE_WH')}"
        f"&role={os.getenv('SNOWFLAKE_ROLE', 'PIPELINE_ROLE')}"
    )
    return create_engine(url)


def get_postgres_engine():
    """Create SQLAlchemy engine for PostgreSQL fallback."""
    from sqlalchemy import create_engine

    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True, pool_size=5)


def export_warehouse_data():
    """Export warehouse queries to CSV. Uses Snowflake if configured, else PostgreSQL."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if SNOWFLAKE_ENABLED:
        logger.info("Querying Snowflake data warehouse...")
        engine = get_snowflake_engine()
        queries = SNOWFLAKE_QUERIES
    else:
        logger.info("Snowflake not configured. Using PostgreSQL fallback...")
        engine = get_postgres_engine()
        queries = PG_QUERIES

    for name, query in queries.items():
        try:
            df = pd.read_sql(query, con=engine)
            filepath = os.path.join(OUTPUT_DIR, f"{name}.csv")
            df.to_csv(filepath, index=False)
            logger.info("Exported %s: %d rows -> %s", name, len(df), filepath)
        except Exception as e:
            logger.error("Failed to export %s: %s", name, e)


def upload_to_tableau():
    """Upload datasets to Tableau via REST API."""
    import requests

    if not TABLEAU_SERVER:
        logger.info("Tableau not configured, skipping.")
        return

    site_id = os.getenv("TABLEAU_SITE_ID", "")
    username = os.getenv("TABLEAU_USERNAME", "admin")
    password = os.getenv("TABLEAU_PASSWORD", "admin")

    try:
        auth_payload = {
            "credentials": {
                "name": username,
                "password": password,
                "site": {"contentUrl": site_id},
            }
        }
        resp = requests.post(
            f"{TABLEAU_SERVER}/api/3.9/auth/signin",
            json=auth_payload,
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["credentials"]["token"]

        for csv_file in os.listdir(OUTPUT_DIR):
            if not csv_file.endswith(".csv"):
                continue
            filepath = os.path.join(OUTPUT_DIR, csv_file)
            with open(filepath, "rb") as f:
                upload_resp = requests.post(
                    f"{TABLEAU_SERVER}/api/3.9/sites/{site_id}/datasources",
                    headers={"X-Tableau-Auth": token},
                    files={"file": f},
                    timeout=60,
                )
                if upload_resp.ok:
                    logger.info("Uploaded %s to Tableau.", csv_file)
                else:
                    logger.error("Tableau upload failed for %s: %s", csv_file, upload_resp.text)

    except Exception as e:
        logger.error("Tableau upload error: %s", e)


def upload_to_looker():
    """Upload datasets to Looker via API."""
    import requests

    if not LOOKER_API_URL:
        logger.info("Looker not configured, skipping.")
        return

    try:
        auth_resp = requests.post(
            f"{LOOKER_API_URL}/login",
            data={
                "client_id": os.getenv("LOOKER_CLIENT_ID", ""),
                "client_secret": os.getenv("LOOKER_CLIENT_SECRET", ""),
            },
            timeout=30,
        )
        auth_resp.raise_for_status()
        logger.info("Authenticated with Looker. Connect Looker to Snowflake directly for best results.")
    except Exception as e:
        logger.error("Looker auth error: %s", e)


def upload_to_power_bi():
    """Push datasets to Power BI via REST API."""
    import requests

    if not POWER_BI_WORKSPACE_ID:
        logger.info("Power BI not configured, skipping.")
        return

    access_token = os.getenv("POWER_BI_ACCESS_TOKEN", "")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        dataset_payload = {
            "name": "Pipeline Warehouse Data",
            "defaultMode": "Push",
            "tables": [
                {
                    "name": "daily_orders",
                    "columns": [
                        {"name": "full_date", "dataType": "DateTime"},
                        {"name": "total_orders", "dataType": "Int64"},
                        {"name": "total_revenue", "dataType": "Double"},
                        {"name": "avg_order_value", "dataType": "Double"},
                        {"name": "unique_customers", "dataType": "Int64"},
                    ],
                }
            ],
        }
        resp = requests.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{POWER_BI_WORKSPACE_ID}/datasets",
            headers=headers,
            json=dataset_payload,
            timeout=30,
        )
        if resp.ok:
            logger.info("Power BI dataset created/updated.")
        else:
            logger.warning("Power BI response: %s", resp.text)
    except Exception as e:
        logger.error("Power BI error: %s", e)


if __name__ == "__main__":
    export_warehouse_data()
    upload_to_tableau()
    upload_to_looker()
    upload_to_power_bi()
