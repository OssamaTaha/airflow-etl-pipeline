"""
DAG 3: transform_and_load_warehouse
Schedule: Daily at 07:00 UTC (after extract DAGs)
Transforms staging data and loads to production warehouse tables.
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, '/opt/airflow/dags')

from utils.db_helpers import (
    execute_non_query,
    check_staging_data_exists,
    log_table_stats,
    execute_query,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ossama_taha",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

# SQL transform queries (from transforms.sql)
EXCHANGE_RATES_TRANSFORM = """
    INSERT INTO production.exchange_rates (base_currency, target_currency, rate, effective_date)
    SELECT DISTINCT ON (base_currency, target_currency, DATE(extraction_timestamp))
        base_currency,
        target_currency,
        rate,
        DATE(extraction_timestamp) AS effective_date
    FROM staging.exchange_rates
    WHERE extraction_timestamp > NOW() - INTERVAL '2 days'
    ORDER BY base_currency, target_currency, DATE(extraction_timestamp), extraction_timestamp DESC
    ON CONFLICT (base_currency, target_currency, effective_date)
    DO UPDATE SET rate = EXCLUDED.rate, loaded_at = NOW();
"""

WORLDBANK_TRANSFORM = """
    INSERT INTO production.country_indicators (
        country_code, country_name, year, gdp_usd, population_total,
        inflation_rate, unemployment_rate
    )
    SELECT
        country_code,
        MAX(country_name) AS country_name,
        year,
        MAX(CASE WHEN indicator_code = 'NY.GDP.MKTP.CD' THEN value END) AS gdp_usd,
        MAX(CASE WHEN indicator_code = 'SP.POP.TOTL' THEN value END)::BIGINT AS population_total,
        MAX(CASE WHEN indicator_code = 'FP.CPI.TOTL.ZG' THEN value END) AS inflation_rate,
        MAX(CASE WHEN indicator_code = 'SL.UEM.TOTL.ZS' THEN value END) AS unemployment_rate
    FROM staging.worldbank_indicators
    WHERE indicator_code IN ('NY.GDP.MKTP.CD', 'SP.POP.TOTL', 'FP.CPI.TOTL.ZG', 'SL.UEM.TOTL.ZS')
      AND year IS NOT NULL
    GROUP BY country_code, year
    ON CONFLICT (country_code, year)
    DO UPDATE SET
        gdp_usd = EXCLUDED.gdp_usd,
        population_total = EXCLUDED.population_total,
        inflation_rate = EXCLUDED.inflation_rate,
        unemployment_rate = EXCLUDED.unemployment_rate,
        loaded_at = NOW();
"""

DERIVED_METRICS_CALC = """
    INSERT INTO production.derived_metrics (metric_name, metric_value, source_tables)
    SELECT
        'gdp_per_capita_' || country_code || '_' || year AS metric_name,
        (gdp_usd / NULLIF(population_total, 0))::NUMERIC(20, 4) AS metric_value,
        ARRAY['production.country_indicators'] AS source_tables
    FROM production.country_indicators
    WHERE gdp_usd IS NOT NULL AND population_total IS NOT NULL
    ON CONFLICT DO NOTHING;
"""


def check_staging_data(**context):
    """Verify that staging tables have fresh data before transforming."""
    checks = {
        "exchange_rates": check_staging_data_exists("staging", "exchange_rates", hours=48),
        "worldbank_indicators": check_staging_data_exists("staging", "worldbank_indicators", hours=168),  # 7 days for weekly
    }

    failed = [k for k, v in checks.items() if not v]
    if failed:
        logger.warning(f"Staging tables missing fresh data: {failed}")
        # Don't fail — still try to transform what's available

    context["ti"].xcom_push(key="staging_status", value=checks)
    logger.info(f"Staging check: {checks}")
    return checks


def clean_exchange_rates(**context):
    """Transform exchange rates: dedup, type cast, load to production."""
    rows = execute_non_query(EXCHANGE_RATES_TRANSFORM)
    log_table_stats("production", "exchange_rates")
    logger.info(f"Transformed {rows} exchange rate records to production")
    return rows


def clean_worldbank(**context):
    """Transform World Bank data: pivot indicators into columns."""
    rows = execute_non_query(WORLDBANK_TRANSFORM)
    log_table_stats("production", "country_indicators")
    logger.info(f"Transformed World Bank data: {rows} records to production")
    return rows


def calculate_derived_metrics(**context):
    """Calculate derived metrics (GDP per capita, etc.)."""
    rows = execute_non_query(DERIVED_METRICS_CALC)
    log_table_stats("production", "derived_metrics")
    logger.info(f"Calculated {rows} derived metrics")
    return rows


def update_data_catalog(**context):
    """Log data catalog entry — row counts, timestamps, table sizes."""
    from utils.db_helpers import log_table_stats

    tables = [
        ("staging", "exchange_rates"),
        ("staging", "worldbank_indicators"),
        ("production", "exchange_rates"),
        ("production", "country_indicators"),
        ("production", "derived_metrics"),
    ]

    for schema, table in tables:
        try:
            log_table_stats(schema, table)
        except Exception as e:
            logger.error(f"Failed to log stats for {schema}.{table}: {e}")

    logger.info("Data catalog updated")


# Define DAG
with DAG(
    dag_id="transform_and_load_warehouse",
    default_args=default_args,
    description="Transform staging data and load to production warehouse",
    schedule_interval="0 7 * * *",  # Daily at 07:00 UTC
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=["transform", "warehouse", "daily"],
    doc_md="""
    ## Transform & Load Warehouse DAG

    Daily transformation pipeline that cleans staging data, pivots World Bank
    indicators, calculates derived metrics, and loads to production tables.
    Depends on extract DAGs completing first.
    """,
) as dag:

    # Task 1: Check staging data exists
    check_staging = PythonOperator(
        task_id="check_staging_data",
        python_callable=check_staging_data,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 2: Clean exchange rates
    transform_er = PythonOperator(
        task_id="clean_exchange_rates",
        python_callable=clean_exchange_rates,
        execution_timeout=timedelta(minutes=10),
    )

    # Task 3: Clean World Bank data
    transform_wb = PythonOperator(
        task_id="clean_worldbank",
        python_callable=clean_worldbank,
        execution_timeout=timedelta(minutes=10),
    )

    # Task 4: Calculate derived metrics (depends on both transforms)
    derive_metrics = PythonOperator(
        task_id="calculate_derived_metrics",
        python_callable=calculate_derived_metrics,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 5: Update data catalog
    update_catalog = PythonOperator(
        task_id="update_data_catalog",
        python_callable=update_data_catalog,
        execution_timeout=timedelta(minutes=5),
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream has warnings
    )

    # Task 6: Log completion
    log_completion = BashOperator(
        task_id="log_transform_complete",
        bash_command='echo "Transform pipeline completed at $(date -u)"',
    )

    # Dependencies: check -> parallel transforms -> derive -> catalog -> done
    check_staging >> [transform_er, transform_wb] >> derive_metrics >> update_catalog >> log_completion
