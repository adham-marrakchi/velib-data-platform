"""
Snowflake Data Warehouse Connector.

Provides connection management, query execution, and data loading
utilities for the Snowflake cloud data warehouse. Used by Airflow DAGs,
the .NET API, and BI dashboard exports.
"""

import logging
import os

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Snowflake Configuration from environment
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "PIPELINE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "PIPELINE_ROLE")


def get_connection():
    """Create a Snowflake connection using snowflake-connector-python."""
    import snowflake.connector

    if not SNOWFLAKE_ACCOUNT or not SNOWFLAKE_USER:
        raise RuntimeError(
            "Snowflake not configured. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, "
            "SNOWFLAKE_PASSWORD environment variables."
        )

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        login_timeout=30,
        network_timeout=60,
    )
    logger.info(
        "Connected to Snowflake: %s.%s.%s",
        SNOWFLAKE_DATABASE,
        SNOWFLAKE_SCHEMA,
        SNOWFLAKE_WAREHOUSE,
    )
    return conn


def execute_sql(sql, params=None):
    """Execute a SQL statement on Snowflake."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        logger.info("Executed SQL: %s...", sql[:80])
        return cursor
    except Exception as e:
        logger.error("Snowflake SQL error: %s", str(e))
        raise
    finally:
        conn.close()


def execute_multi(sql_statements):
    """Execute multiple SQL statements in a single connection."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        for sql in sql_statements:
            sql = sql.strip()
            if sql:
                cursor.execute(sql)
                logger.info("Executed: %s...", sql[:60])
        logger.info("Executed %d statements.", len(sql_statements))
    except Exception as e:
        logger.error("Snowflake multi-SQL error: %s", str(e))
        raise
    finally:
        conn.close()


def query_to_dataframe(sql, params=None):
    """Execute a query and return results as a pandas DataFrame."""
    conn = get_connection()
    try:
        df = pd.read_sql(sql, conn, params=params)
        logger.info("Query returned %d rows.", len(df))
        return df
    except Exception as e:
        logger.error("Snowflake query error: %s", str(e))
        raise
    finally:
        conn.close()


def load_dataframe(df, table_name, database=None, schema=None, overwrite=False):
    """Load a pandas DataFrame into a Snowflake table using write_pandas."""
    from snowflake.connector.pandas_tools import write_pandas

    conn = get_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=database or SNOWFLAKE_DATABASE,
            schema=schema or SNOWFLAKE_SCHEMA,
            overwrite=overwrite,
            auto_create_table=False,
        )
        logger.info(
            "Loaded %d rows into %s.%s.%s (%d chunks).",
            nrows,
            database or SNOWFLAKE_DATABASE,
            schema or SNOWFLAKE_SCHEMA,
            table_name,
            nchunks,
        )
        return nrows
    except Exception as e:
        logger.error("Snowflake load error for %s: %s", table_name, str(e))
        raise
    finally:
        conn.close()


def stage_and_copy(local_file_path, table_name, stage_name="PIPELINE_STAGE"):
    """Upload a local file to a Snowflake stage and COPY INTO table."""
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # PUT file to stage
        put_sql = f"PUT file://{local_file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cursor.execute(put_sql)
        logger.info("Staged file: %s -> @%s", local_file_path, stage_name)

        # COPY INTO table
        filename = os.path.basename(local_file_path)
        copy_sql = f"""
            COPY INTO {table_name}
            FROM @{stage_name}/{filename}
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_sql)
        logger.info("COPY INTO %s completed.", table_name)

    except Exception as e:
        logger.error("Snowflake stage/copy error: %s", str(e))
        raise
    finally:
        conn.close()


def check_connection():
    """Verify Snowflake connectivity. Returns True if connected."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        conn.close()
        logger.info("Snowflake connected. Version: %s", version)
        return True
    except Exception as e:
        logger.error("Snowflake connection check failed: %s", str(e))
        return False


if __name__ == "__main__":
    if check_connection():
        print("Snowflake connection successful.")
    else:
        print("Snowflake connection failed. Check configuration.")
