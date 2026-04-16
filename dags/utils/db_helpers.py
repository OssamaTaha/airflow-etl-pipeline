"""
PostgreSQL utility functions for the ETL pipeline.
"""
import os
import logging
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import pandas as pd

logger = logging.getLogger(__name__)


def get_connection_string() -> str:
    """Get PostgreSQL connection string from Airflow connection or env."""
    return os.environ.get(
        "AIRFLOW_CONN_POSTGRES_TARGET",
        "postgresql://target_user:target_pass@postgres:5432/etl_target",
    )


@contextmanager
def get_db_connection():
    """Context manager for PostgreSQL connection."""
    conn_string = get_connection_string()
    conn = psycopg2.connect(conn_string)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a SELECT query and return results as list of dicts."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def execute_non_query(query: str, params: Optional[tuple] = None) -> int:
    """Execute INSERT/UPDATE/DELETE and return affected rows."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount


def insert_dataframe(
    df: pd.DataFrame,
    schema: str,
    table: str,
    if_exists: str = "append",
) -> int:
    """Insert a pandas DataFrame into PostgreSQL."""
    from sqlalchemy import create_engine

    engine = create_engine(get_connection_string())
    rows = df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
    )
    logger.info(f"Inserted {rows} rows into {schema}.{table}")
    return rows


def get_table_row_count(schema: str, table: str) -> int:
    """Get row count for a table."""
    result = execute_query(
        f"SELECT COUNT(*) as cnt FROM {schema}.{table}"
    )
    return result[0]["cnt"] if result else 0


def get_table_size_bytes(schema: str, table: str) -> int:
    """Get table size in bytes."""
    result = execute_query(
        "SELECT pg_total_relation_size(%s) as size",
        (f"{schema}.{table}",),
    )
    return result[0]["size"] if result else 0


def check_staging_data_exists(
    schema: str = "staging",
    table: str = "exchange_rates",
    hours: int = 24,
) -> bool:
    """Check if staging table has data from the last N hours."""
    result = execute_query(
        f"""
        SELECT COUNT(*) as cnt
        FROM {schema}.{table}
        WHERE extraction_timestamp > NOW() - INTERVAL '{hours} hours'
        """
    )
    count = result[0]["cnt"] if result else 0
    logger.info(f"Staging {schema}.{table} has {count} recent rows")
    return count > 0


def log_pipeline_run(
    dag_id: str,
    run_id: str,
    state: str,
    start_time: str,
    end_time: str = None,
    duration_seconds: float = None,
):
    """Log a pipeline run to monitoring table."""
    execute_non_query(
        """
        INSERT INTO monitoring.pipeline_runs
            (dag_id, run_id, state, start_time, end_time, duration_seconds)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (dag_id, run_id, state, start_time, end_time, duration_seconds),
    )


def log_table_stats(schema: str, table: str):
    """Record table statistics to monitoring."""
    row_count = get_table_row_count(schema, table)
    size_bytes = get_table_size_bytes(schema, table)
    execute_non_query(
        """
        INSERT INTO monitoring.table_stats
            (schema_name, table_name, row_count, table_size_bytes)
        VALUES (%s, %s, %s, %s)
        """,
        (schema, table, row_count, size_bytes),
    )
