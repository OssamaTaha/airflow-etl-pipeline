"""
Tests for transformation logic.
Run with: pytest tests/test_transform.py -v
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))


def test_exchange_rate_transform_sql():
    """Verify the exchange rate transform SQL is valid."""
    from transform_and_load_warehouse import EXCHANGE_RATES_TRANSFORM
    assert "INSERT INTO production.exchange_rates" in EXCHANGE_RATES_TRANSFORM
    assert "DISTINCT ON" in EXCHANGE_RATES_TRANSFORM
    assert "ON CONFLICT" in EXCHANGE_RATES_TRANSFORM


def test_worldbank_transform_sql():
    """Verify the World Bank transform SQL is valid."""
    from transform_and_load_warehouse import WORLDBANK_TRANSFORM
    assert "INSERT INTO production.country_indicators" in WORLDBANK_TRANSFORM
    assert "MAX(CASE WHEN" in WORLDBANK_TRANSFORM
    assert "GROUP BY country_code, year" in WORLDBANK_TRANSFORM


def test_derived_metrics_sql():
    """Verify derived metrics SQL."""
    from transform_and_load_warehouse import DERIVED_METRICS_CALC
    assert "gdp_per_capita" in DERIVED_METRICS_CALC
    assert "NULLIF" in DERIVED_METRICS_CALC


def test_dag_task_count():
    """Verify transform DAG has expected task count."""
    from transform_and_load_warehouse import dag
    task_ids = [t.task_id for t in dag.tasks]
    # check_staging, clean_er, clean_wb, derive_metrics, update_catalog, log_completion = 6
    assert len(task_ids) == 6


def test_dag_task_dependencies():
    """Verify transform DAG has correct parallel structure."""
    from transform_and_load_warehouse import dag

    check_staging = dag.task_dict["check_staging_data"]
    clean_er = dag.task_dict["clean_exchange_rates"]
    clean_wb = dag.task_dict["clean_worldbank"]
    derive = dag.task_dict["calculate_derived_metrics"]

    # check_staging feeds both transforms
    assert clean_er in check_staging.downstream_list
    assert clean_wb in check_staging.downstream_list

    # Both transforms feed derive
    assert derive in clean_er.downstream_list
    assert derive in clean_wb.downstream_list


def test_db_helpers_module():
    """Verify db_helpers module loads correctly."""
    from utils.db_helpers import get_connection_string, get_table_row_count
    conn_str = get_connection_string()
    assert "postgresql://" in conn_str
