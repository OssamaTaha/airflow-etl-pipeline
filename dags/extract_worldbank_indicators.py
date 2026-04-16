"""
DAG 2: extract_worldbank_indicators
Schedule: Weekly (Sunday 02:00 UTC)
Extracts economic indicators from World Bank API using dynamic task mapping.
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task

import sys
sys.path.insert(0, '/opt/airflow/dags')

from utils.api_clients import WorldBankClient
from utils.validators import validate_worldbank_records
from utils.db_helpers import execute_non_query, log_table_stats

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ossama_taha",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=15),
}

# Countries to extract (Middle East focus — shows regional knowledge)
COUNTRY_CODES = ["EGY", "SAU", "ARE", "QAT", "JOR", "LBN", "IRQ", "OMN", "KWT", "BHR"]

# Indicators to extract
INDICATOR_CODES = [
    "NY.GDP.MKTP.CD",     # GDP (current US$)
    "SP.POP.TOTL",        # Population total
    "FP.CPI.TOTL.ZG",     # Inflation, consumer prices (annual %)
    "SL.UEM.TOTL.ZS",     # Unemployment, total (% of labor force)
]


def get_country_list(**context):
    """Fetch list of countries and validate they exist in the API."""
    client = WorldBankClient()
    available = client.get_countries()
    available_codes = {c.get("id", "")[:3] for c in available}

    # Validate our target countries exist
    missing = [c for c in COUNTRY_CODES if c not in available_codes]
    if missing:
        logger.warning(f"Countries not found in API: {missing}")

    context["ti"].xcom_push(key="country_codes", value=COUNTRY_CODES)
    logger.info(f"Validated {len(COUNTRY_CODES)} countries")
    return COUNTRY_CODES


@task(task_id="fetch_indicators", map_index_arg="indicator_idx")
def fetch_indicators_task(indicator: str, **context):
    """
    Dynamic task: fetch one indicator for all countries.
    Uses .expand() for parallel execution.
    """
    client = WorldBankClient()
    ti = context["ti"]
    country_codes = ti.xcom_pull(task_ids="get_country_list", key="country_codes") or COUNTRY_CODES

    records = client.get_indicators(
        country_codes=country_codes,
        indicator_codes=[indicator],
        start_year=2020,
        end_year=2025,
    )

    # Validate records
    is_valid, errors = validate_worldbank_records(records)
    if not is_valid:
        logger.error(f"Validation failed for {indicator}: {errors[:3]}")
        # Still return what we have — partial data is better than no data

    # Filter out null values
    valid_records = [r for r in records if r.get("value") is not None]
    logger.info(f"Fetched {len(valid_records)} valid records for {indicator}")

    return valid_records


def merge_and_load_staging(**context):
    """Merge all indicator results and load to staging table."""
    ti = context["ti"]

    # Pull results from all dynamic task instances
    all_records = []
    for indicator in INDICATOR_CODES:
        try:
            records = ti.xcom_pull(
                task_ids=f"fetch_indicators_task",
                map_indexes=INDICATOR_CODES.index(indicator),
            )
            if records:
                all_records.extend(records)
        except Exception as e:
            logger.error(f"Failed to pull results for {indicator}: {e}")

    if not all_records:
        raise ValueError("No indicator data received from upstream tasks")

    # Load to staging
    total_inserted = 0
    for record in all_records:
        try:
            execute_non_query(
                """
                INSERT INTO staging.worldbank_indicators
                    (country_code, country_name, indicator_code, indicator_name, year, value)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    record.get("country_code", ""),
                    record.get("country_name", ""),
                    record.get("indicator_code", ""),
                    record.get("indicator_name", ""),
                    record.get("year"),
                    record.get("value"),
                ),
            )
            total_inserted += 1
        except Exception as e:
            logger.error(f"Failed to insert record: {e}")

    log_table_stats("staging", "worldbank_indicators")

    logger.info(f"Loaded {total_inserted} indicator records to staging")
    return total_inserted


# Define DAG
with DAG(
    dag_id="extract_worldbank_indicators",
    default_args=default_args,
    description="Extract economic indicators from World Bank API with dynamic task mapping",
    schedule_interval="0 2 * * 0",  # Weekly on Sunday at 02:00 UTC
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=["extract", "worldbank", "weekly", "dynamic-mapping"],
    doc_md="""
    ## World Bank Indicators Extraction DAG

    Extracts GDP, population, inflation, and unemployment data for Middle East
    countries using Airflow's dynamic task mapping (.expand()) for parallel
    extraction per indicator.
    """,
) as dag:

    # Task 1: Get and validate country list
    get_countries = PythonOperator(
        task_id="get_country_list",
        python_callable=get_country_list,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 2: Dynamic extraction — one task per indicator
    fetch_tasks = fetch_indicators_task.expand(indicator=INDICATOR_CODES)

    # Task 3: Merge results and load to staging
    merge_load = PythonOperator(
        task_id="merge_and_load_staging",
        python_callable=merge_and_load_staging,
        execution_timeout=timedelta(minutes=10),
    )

    # Task 4: Log completion
    log_completion = BashOperator(
        task_id="log_extraction_complete",
        bash_command='echo "World Bank extraction completed at $(date -u)"',
    )

    # Dependencies
    get_countries >> fetch_tasks >> merge_load >> log_completion
