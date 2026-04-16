"""
DAG 1: extract_exchange_rates
Schedule: Daily at 06:00 UTC
Extracts currency exchange rates from open.er-api.com and loads to staging.
"""
from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor

import sys
sys.path.insert(0, '/opt/airflow/dags')

from utils.api_clients import ExchangeRateClient
from utils.validators import validate_exchange_rate_response
from utils.db_helpers import execute_non_query, log_table_stats

logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    "owner": "ossama_taha",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=10),
}

# Base currencies to extract
BASE_CURRENCIES = ["USD", "EUR", "EGP"]


def check_api_health(**context):
    """Check if the exchange rate API is available."""
    import requests
    try:
        response = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        response.raise_for_status()
        logger.info("Exchange rate API is healthy")
        return True
    except Exception as e:
        logger.error(f"API health check failed: {e}")
        raise


def fetch_exchange_rates(**context):
    """Fetch exchange rates for all base currencies."""
    client = ExchangeRateClient()
    all_rates = {}

    for base in BASE_CURRENCIES:
        try:
            data = client.get_rates(base)
            if validate_exchange_rate_response(data):
                all_rates[base] = data
                logger.info(f"Validated rates for {base}: {len(data.get('rates', {}))} currencies")
            else:
                logger.error(f"Validation failed for {base}")
                raise ValueError(f"Invalid response for {base}")
        except Exception as e:
            logger.error(f"Failed to fetch {base}: {e}")
            raise

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="exchange_rates", value=all_rates)
    logger.info(f"Fetched rates for {len(all_rates)} base currencies")
    return len(all_rates)


def validate_and_load_staging(**context):
    """Validate fetched rates and load to staging table."""
    ti = context["ti"]
    all_rates = ti.xcom_pull(task_ids="fetch_exchange_rates", key="exchange_rates")

    if not all_rates:
        raise ValueError("No exchange rate data received from upstream task")

    total_inserted = 0

    for base, data in all_rates.items():
        rates = data.get("rates", {})
        timestamp = data.get("time_last_update_utc", datetime.utcnow().isoformat())

        for target, rate in rates.items():
            try:
                execute_non_query(
                    """
                    INSERT INTO staging.exchange_rates
                        (base_currency, target_currency, rate, extraction_timestamp, raw_response)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (base, target, float(rate), timestamp, json.dumps(data)),
                )
                total_inserted += 1
            except Exception as e:
                logger.error(f"Failed to insert {base}->{target}: {e}")

    # Log table stats
    log_table_stats("staging", "exchange_rates")

    logger.info(f"Loaded {total_inserted} rate records to staging")
    return total_inserted


# Define DAG
with DAG(
    dag_id="extract_exchange_rates",
    default_args=default_args,
    description="Extract currency exchange rates from open.er-api.com",
    schedule_interval="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=["extract", "currency", "daily"],
    doc_md="""
    ## Exchange Rate Extraction DAG

    Extracts daily currency exchange rates for USD, EUR, and EGP base currencies
    from the free open.er-api.com API. Loads raw data into staging.exchange_rates.
    """,
) as dag:

    # Task 1: Check API availability
    check_api = PythonOperator(
        task_id="check_api_availability",
        python_callable=check_api_health,
        execution_timeout=timedelta(minutes=2),
    )

    # Task 2: Fetch exchange rates
    fetch_rates = PythonOperator(
        task_id="fetch_exchange_rates",
        python_callable=fetch_exchange_rates,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 3: Validate and load to staging
    validate_load = PythonOperator(
        task_id="validate_and_load_staging",
        python_callable=validate_and_load_staging,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 4: Log extraction completion
    log_completion = BashOperator(
        task_id="log_extraction_complete",
        bash_command='echo "Exchange rate extraction completed at $(date -u)"',
    )

    # Task dependencies
    check_api >> fetch_rates >> validate_load >> log_completion
