"""
DAG 4: data_pipeline_monitoring
Schedule: Every 6 hours
Monitors pipeline health, table sizes, and DAG run history.
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.insert(0, '/opt/airflow/dags')

from utils.db_helpers import execute_query, execute_non_query, log_table_stats

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ossama_taha",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
}


def check_dag_health(**context):
    """Query Airflow metadata for DAG run health."""
    # Check recent DAG runs from monitoring table
    try:
        recent_runs = execute_query(
            """
            SELECT dag_id, state, COUNT(*) as run_count,
                   AVG(duration_seconds) as avg_duration,
                   MAX(end_time) as last_run
            FROM monitoring.pipeline_runs
            WHERE recorded_at > NOW() - INTERVAL '7 days'
            GROUP BY dag_id, state
            ORDER BY dag_id, state
            """
        )

        health_report = []
        for run in recent_runs:
            health_report.append({
                "dag": run["dag_id"],
                "state": run["state"],
                "count": run["run_count"],
                "avg_duration": round(float(run["avg_duration"] or 0), 2),
                "last_run": str(run["last_run"]) if run["last_run"] else "N/A",
            })

        logger.info(f"DAG health report: {len(health_report)} entries")
        for entry in health_report:
            logger.info(f"  {entry['dag']} [{entry['state']}]: {entry['count']} runs, "
                       f"avg {entry['avg_duration']}s, last: {entry['last_run']}")

        context["ti"].xcom_push(key="health_report", value=health_report)
        return health_report

    except Exception as e:
        logger.error(f"Failed to check DAG health: {e}")
        return []


def check_postgres_size(**context):
    """Monitor PostgreSQL table growth and database size."""
    try:
        # Database total size
        db_size = execute_query(
            "SELECT pg_size_pretty(pg_database_size('etl_target')) as size"
        )

        # Per-table sizes for key tables
        table_sizes = execute_query(
            """
            SELECT
                schemaname || '.' || tablename as full_name,
                pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as size,
                pg_total_relation_size(schemaname || '.' || tablename) as size_bytes
            FROM pg_tables
            WHERE schemaname IN ('staging', 'production', 'monitoring')
            ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
            """
        )

        # Row counts
        row_counts = execute_query(
            """
            SELECT schemaname, relname as tablename, n_live_tup as row_count
            FROM pg_stat_user_tables
            WHERE schemaname IN ('staging', 'production', 'monitoring')
            ORDER BY n_live_tup DESC
            """
        )

        size_report = {
            "database_size": db_size[0]["size"] if db_size else "unknown",
            "tables": [
                {
                    "name": t["full_name"],
                    "size": t["size"],
                    "size_bytes": t["size_bytes"],
                }
                for t in table_sizes
            ],
            "row_counts": [
                {
                    "schema": r["schemaname"],
                    "table": r["tablename"],
                    "rows": r["row_count"],
                }
                for r in row_counts
            ],
        }

        # Log stats for all tables
        for t in table_sizes:
            parts = t["full_name"].split(".")
            if len(parts) == 2:
                log_table_stats(parts[0], parts[1])

        logger.info(f"Database size: {size_report['database_size']}")
        logger.info(f"Top tables by size:")
        for t in size_report["tables"][:5]:
            logger.info(f"  {t['name']}: {t['size']}")

        context["ti"].xcom_push(key="size_report", value=size_report)
        return size_report

    except Exception as e:
        logger.error(f"Failed to check PostgreSQL size: {e}")
        return {}


def generate_health_report(**context):
    """Generate a health summary and write to monitoring table."""
    ti = context["ti"]
    health = ti.xcom_pull(task_ids="check_dag_health", key="health_report") or []
    size = ti.xcom_pull(task_ids="check_postgres_size", key="size_report") or {}

    # Write summary to quality_checks table
    total_checks = 0
    passed = 0

    # Check 1: DAG health
    for entry in health:
        total_checks += 1
        status = "PASS" if entry.get("state") == "success" else "WARN"
        if status == "PASS":
            passed += 1
        execute_non_query(
            """
            INSERT INTO monitoring.quality_checks (check_name, table_name, status, message)
            VALUES (%s, %s, %s, %s)
            """,
            (
                f"dag_health_{entry['dag']}",
                "pipeline_runs",
                status,
                f"{entry['count']} runs, avg {entry['avg_duration']}s",
            ),
        )

    # Check 2: Table sizes
    for table in size.get("tables", []):
        total_checks += 1
        # Tables over 1GB are notable
        size_bytes = table.get("size_bytes", 0)
        status = "PASS" if size_bytes < 1_000_000_000 else "WARN"
        if status == "PASS":
            passed += 1
        execute_non_query(
            """
            INSERT INTO monitoring.quality_checks (check_name, table_name, status, message)
            VALUES (%s, %s, %s, %s)
            """,
            (
                f"table_size_{table['name']}",
                table["name"],
                status,
                f"Size: {table['size']}",
            ),
        )

    summary = f"Health check: {passed}/{total_checks} passed"
    logger.info(summary)

    # Write overall summary
    execute_non_query(
        """
        INSERT INTO monitoring.quality_checks (check_name, table_name, status, message)
        VALUES (%s, %s, %s, %s)
        """,
        (
            "overall_pipeline_health",
            "ALL",
            "PASS" if passed == total_checks else "WARN",
            summary,
        ),
    )

    return summary


# Define DAG
with DAG(
    dag_id="data_pipeline_monitoring",
    default_args=default_args,
    description="Monitor pipeline health, table growth, and run history",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=["monitoring", "health", "operational"],
    doc_md="""
    ## Pipeline Monitoring DAG

    Runs every 6 hours to check DAG health, PostgreSQL table sizes,
    and pipeline run history. Generates quality check reports for
    the Streamlit dashboard.
    """,
) as dag:

    # Task 1: Check DAG run health
    check_health = PythonOperator(
        task_id="check_dag_health",
        python_callable=check_dag_health,
        execution_timeout=timedelta(minutes=3),
    )

    # Task 2: Check PostgreSQL size
    check_size = PythonOperator(
        task_id="check_postgres_size",
        python_callable=check_postgres_size,
        execution_timeout=timedelta(minutes=3),
    )

    # Task 3: Generate health report
    gen_report = PythonOperator(
        task_id="generate_health_report",
        python_callable=generate_health_report,
        execution_timeout=timedelta(minutes=3),
    )

    # Task 4: Log completion
    log_completion = BashOperator(
        task_id="log_monitoring_complete",
        bash_command='echo "Monitoring check completed at $(date -u)"',
    )

    # Dependencies
    [check_health, check_size] >> gen_report >> log_completion
