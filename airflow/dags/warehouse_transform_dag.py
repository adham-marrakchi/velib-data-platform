"""
Snowflake Data Warehouse ETL DAG.

Extracts transformed data from PostgreSQL (staging), loads into Snowflake
warehouse star schema. Runs dimensions first, then facts, then aggregations.

Flow: PostgreSQL (staging) → Snowflake (dimensions → facts → aggregations)
"""

import logging
import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow import DAG

logger = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_ENABLED = bool(SNOWFLAKE_ACCOUNT)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def _get_snowflake_conn():
    """Get Snowflake connection (lazy import to avoid DAG parse errors)."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "PIPELINE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
        role=os.getenv("SNOWFLAKE_ROLE", "PIPELINE_ROLE"),
    )


def _get_pg():
    """Get PostgreSQL hook."""
    return PostgresHook(postgres_conn_id="postgres_default")


def extract_and_stage_orders(**kwargs):
    """Extract transformed orders from PostgreSQL and stage in Snowflake."""
    pg = _get_pg()
    df = pg.get_pandas_df("SELECT * FROM orders_transformed")
    logger.info("Extracted %d orders from PostgreSQL.", len(df))

    if df.empty:
        logger.warning("No orders to stage.")
        return 0

    if SNOWFLAKE_ENABLED:
        from snowflake.connector.pandas_tools import write_pandas

        conn = _get_snowflake_conn()
        try:
            # Truncate staging table first
            conn.cursor().execute("TRUNCATE TABLE STAGING.STG_ORDERS")
            df.columns = [c.upper() for c in df.columns]
            if "LOADED_AT" not in df.columns:
                df["LOADED_AT"] = datetime.utcnow()
            _, _, nrows, _ = write_pandas(conn, df, "STG_ORDERS", database="PIPELINE_DB", schema="STAGING")
            logger.info("Staged %d orders in Snowflake.", nrows)
        finally:
            conn.close()
    else:
        logger.info("Snowflake disabled. Using PostgreSQL warehouse tables.")
        pg.run("""
            INSERT INTO dim_customers (customer_id, customer_name, join_date)
            SELECT DISTINCT ot.customer_id, 'Customer_' || ot.customer_id, MIN(ot.processed_timestamp)
            FROM orders_transformed ot
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_customers dc WHERE dc.customer_id = ot.customer_id AND dc.is_current = TRUE
            )
            GROUP BY ot.customer_id
            ON CONFLICT (customer_id) DO NOTHING
        """)

    return len(df)


def extract_and_stage_anomalies(**kwargs):
    """Extract anomaly stream data from PostgreSQL and stage in Snowflake."""
    pg = _get_pg()

    try:
        df = pg.get_pandas_df("SELECT * FROM anomalies_stream")
    except Exception:
        logger.warning("anomalies_stream table not found. Skipping.")
        return 0

    logger.info("Extracted %d anomalies from PostgreSQL.", len(df))

    if df.empty:
        return 0

    if SNOWFLAKE_ENABLED:
        from snowflake.connector.pandas_tools import write_pandas

        conn = _get_snowflake_conn()
        try:
            conn.cursor().execute("TRUNCATE TABLE STAGING.STG_SENSOR_READINGS")
            df.columns = [c.upper() for c in df.columns]
            if "LOADED_AT" not in df.columns:
                df["LOADED_AT"] = datetime.utcnow()
            _, _, nrows, _ = write_pandas(conn, df, "STG_SENSOR_READINGS", database="PIPELINE_DB", schema="STAGING")
            logger.info("Staged %d anomalies in Snowflake.", nrows)
        finally:
            conn.close()
    else:
        pg.run("""
            INSERT INTO dim_devices (device_id, device_type)
            SELECT DISTINCT a.device_id::INT, 'sensor'
            FROM anomalies_stream a
            WHERE a.device_id IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM dim_devices dd WHERE dd.device_id = a.device_id::INT)
            ON CONFLICT (device_id) DO NOTHING
        """)

    return len(df)


def load_dimensions(**kwargs):
    """Load dimension tables in Snowflake from staging data."""
    if not SNOWFLAKE_ENABLED:
        logger.info("Snowflake disabled. Dimensions loaded in staging step (PostgreSQL).")
        return

    conn = _get_snowflake_conn()
    try:
        cursor = conn.cursor()

        # Load DIM_CUSTOMERS from staging
        cursor.execute("""
            MERGE INTO ANALYTICS.DIM_CUSTOMERS tgt
            USING (
                SELECT DISTINCT CUSTOMER_ID, 'Customer_' || CUSTOMER_ID AS CUSTOMER_NAME,
                       MIN(PROCESSED_TIMESTAMP) AS JOIN_DATE
                FROM STAGING.STG_ORDERS
                GROUP BY CUSTOMER_ID
            ) src ON tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = TRUE
            WHEN NOT MATCHED THEN INSERT (CUSTOMER_ID, CUSTOMER_NAME, JOIN_DATE)
                VALUES (src.CUSTOMER_ID, src.CUSTOMER_NAME, src.JOIN_DATE)
        """)
        logger.info("DIM_CUSTOMERS loaded.")

        # Load DIM_DEVICES from staging
        cursor.execute("""
            MERGE INTO ANALYTICS.DIM_DEVICES tgt
            USING (
                SELECT DISTINCT DEVICE_ID, 'sensor' AS DEVICE_TYPE
                FROM STAGING.STG_SENSOR_READINGS
                WHERE DEVICE_ID IS NOT NULL
            ) src ON tgt.DEVICE_ID = src.DEVICE_ID
            WHEN NOT MATCHED THEN INSERT (DEVICE_ID, DEVICE_TYPE)
                VALUES (src.DEVICE_ID, src.DEVICE_TYPE)
        """)
        logger.info("DIM_DEVICES loaded.")

    finally:
        conn.close()


def load_facts(**kwargs):
    """Load fact tables in Snowflake from staging data."""
    if not SNOWFLAKE_ENABLED:
        pg = _get_pg()
        pg.run("""
            INSERT INTO fact_orders (order_id, customer_key, date_key, order_amount, net_amount, processed_timestamp)
            SELECT ot.order_id, dc.customer_key, TO_CHAR(ot.processed_timestamp, 'YYYYMMDD')::INT,
                   ot.amount, ot.amount, ot.processed_timestamp
            FROM orders_transformed ot
            JOIN dim_customers dc ON dc.customer_id = ot.customer_id AND dc.is_current = TRUE
            LEFT JOIN fact_orders fo ON fo.order_id = ot.order_id
            WHERE fo.order_key IS NULL
        """)
        logger.info("Fact orders loaded (PostgreSQL fallback).")
        return

    conn = _get_snowflake_conn()
    try:
        cursor = conn.cursor()

        # Load FACT_ORDERS
        cursor.execute("""
            INSERT INTO ANALYTICS.FACT_ORDERS (ORDER_ID, CUSTOMER_KEY, DATE_KEY, ORDER_AMOUNT, NET_AMOUNT, PROCESSED_TIMESTAMP)
            SELECT
                s.ORDER_ID,
                dc.CUSTOMER_KEY,
                TO_NUMBER(TO_CHAR(s.PROCESSED_TIMESTAMP, 'YYYYMMDD')),
                s.AMOUNT,
                s.AMOUNT,
                s.PROCESSED_TIMESTAMP
            FROM STAGING.STG_ORDERS s
            JOIN ANALYTICS.DIM_CUSTOMERS dc ON dc.CUSTOMER_ID = s.CUSTOMER_ID AND dc.IS_CURRENT = TRUE
            LEFT JOIN ANALYTICS.FACT_ORDERS fo ON fo.ORDER_ID = s.ORDER_ID
            WHERE fo.ORDER_KEY IS NULL
        """)
        logger.info("FACT_ORDERS loaded.")

        # Load FACT_SENSOR_READINGS
        cursor.execute("""
            INSERT INTO ANALYTICS.FACT_SENSOR_READINGS
                (DEVICE_KEY, DATE_KEY, EVENT_ID, READING_VALUE, IS_ANOMALY, READING_TIMESTAMP, PROCESSED_TIMESTAMP)
            SELECT
                dd.DEVICE_KEY,
                TO_NUMBER(TO_CHAR(COALESCE(s.PROCESSED_AT, CURRENT_TIMESTAMP()), 'YYYYMMDD')),
                s.EVENT_ID,
                s.READING_VALUE,
                COALESCE(s.IS_ANOMALY, TRUE),
                s.READING_TIMESTAMP,
                COALESCE(s.PROCESSED_AT, CURRENT_TIMESTAMP())
            FROM STAGING.STG_SENSOR_READINGS s
            JOIN ANALYTICS.DIM_DEVICES dd ON dd.DEVICE_ID = s.DEVICE_ID
            LEFT JOIN ANALYTICS.FACT_SENSOR_READINGS fsr ON fsr.EVENT_ID = s.EVENT_ID
            WHERE fsr.READING_KEY IS NULL
        """)
        logger.info("FACT_SENSOR_READINGS loaded.")

    finally:
        conn.close()


def refresh_aggregations(**kwargs):
    """Refresh aggregation tables (Snowflake Task handles this, but run on-demand too)."""
    if not SNOWFLAKE_ENABLED:
        pg = _get_pg()
        pg.run("""
            INSERT INTO agg_daily_orders (date_key, total_orders, total_revenue, avg_order_value,
                                          unique_customers, max_order_value, min_order_value, updated_at)
            SELECT fo.date_key, COUNT(*), SUM(fo.net_amount), AVG(fo.net_amount),
                   COUNT(DISTINCT fo.customer_key), MAX(fo.net_amount), MIN(fo.net_amount), CURRENT_TIMESTAMP
            FROM fact_orders fo GROUP BY fo.date_key
            ON CONFLICT (date_key) DO UPDATE SET
                total_orders = EXCLUDED.total_orders, total_revenue = EXCLUDED.total_revenue,
                avg_order_value = EXCLUDED.avg_order_value, unique_customers = EXCLUDED.unique_customers,
                max_order_value = EXCLUDED.max_order_value, min_order_value = EXCLUDED.min_order_value,
                updated_at = CURRENT_TIMESTAMP
        """)
        logger.info("Daily aggregation refreshed (PostgreSQL fallback).")
        return

    conn = _get_snowflake_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            MERGE INTO ANALYTICS.AGG_DAILY_ORDERS tgt
            USING (
                SELECT DATE_KEY, COUNT(*) AS TOTAL_ORDERS, SUM(NET_AMOUNT) AS TOTAL_REVENUE,
                       AVG(NET_AMOUNT) AS AVG_ORDER_VALUE, COUNT(DISTINCT CUSTOMER_KEY) AS UNIQUE_CUSTOMERS,
                       MAX(NET_AMOUNT) AS MAX_ORDER_VALUE, MIN(NET_AMOUNT) AS MIN_ORDER_VALUE
                FROM ANALYTICS.FACT_ORDERS GROUP BY DATE_KEY
            ) src ON tgt.DATE_KEY = src.DATE_KEY
            WHEN MATCHED THEN UPDATE SET
                tgt.TOTAL_ORDERS = src.TOTAL_ORDERS, tgt.TOTAL_REVENUE = src.TOTAL_REVENUE,
                tgt.AVG_ORDER_VALUE = src.AVG_ORDER_VALUE, tgt.UNIQUE_CUSTOMERS = src.UNIQUE_CUSTOMERS,
                tgt.MAX_ORDER_VALUE = src.MAX_ORDER_VALUE, tgt.MIN_ORDER_VALUE = src.MIN_ORDER_VALUE,
                tgt.UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT VALUES (
                src.DATE_KEY, src.TOTAL_ORDERS, src.TOTAL_REVENUE, src.AVG_ORDER_VALUE,
                src.UNIQUE_CUSTOMERS, src.MAX_ORDER_VALUE, src.MIN_ORDER_VALUE, CURRENT_TIMESTAMP()
            )
        """)
        logger.info("AGG_DAILY_ORDERS refreshed in Snowflake.")
    finally:
        conn.close()


def log_pipeline_run(**kwargs):
    """Log this pipeline run to the operational fact table."""
    run_id = kwargs.get("run_id", "manual")

    if SNOWFLAKE_ENABLED:
        conn = _get_snowflake_conn()
        try:
            conn.cursor().execute(
                "INSERT INTO ANALYTICS.FACT_PIPELINE_RUNS (DAG_ID, RUN_ID, RUN_TYPE, START_TIME, STATUS) "
                "VALUES (%s, %s, 'scheduled', CURRENT_TIMESTAMP(), 'success')",
                ("warehouse_transform_dag", run_id),
            )
        finally:
            conn.close()
    else:
        pg = _get_pg()
        pg.run(
            "INSERT INTO fact_pipeline_runs (dag_id, run_id, run_type, start_time, status) "
            "VALUES ('warehouse_transform_dag', %s, 'scheduled', CURRENT_TIMESTAMP, 'success')",
            parameters=(run_id,),
        )

    logger.info("Pipeline run logged (target: %s).", "Snowflake" if SNOWFLAKE_ENABLED else "PostgreSQL")


with DAG(
    dag_id="warehouse_transform_dag",
    default_args=default_args,
    description="Snowflake Data Warehouse ETL: Stage → Dimensions → Facts → Aggregations",
    schedule="@hourly",
    catchup=False,
    tags=["warehouse", "etl", "snowflake", "star-schema"],
) as dag:

    stage_orders = PythonOperator(
        task_id="extract_and_stage_orders",
        python_callable=extract_and_stage_orders,
        execution_timeout=timedelta(minutes=15),
    )

    stage_anomalies = PythonOperator(
        task_id="extract_and_stage_anomalies",
        python_callable=extract_and_stage_anomalies,
        execution_timeout=timedelta(minutes=15),
    )

    dims = PythonOperator(
        task_id="load_dimensions",
        python_callable=load_dimensions,
        execution_timeout=timedelta(minutes=10),
    )

    facts = PythonOperator(
        task_id="load_facts",
        python_callable=load_facts,
        execution_timeout=timedelta(minutes=15),
    )

    aggs = PythonOperator(
        task_id="refresh_aggregations",
        python_callable=refresh_aggregations,
        execution_timeout=timedelta(minutes=10),
    )

    log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
    )

    # Stage in parallel, then dimensions, facts, aggregations, log
    [stage_orders, stage_anomalies] >> dims >> facts >> aggs >> log_run
