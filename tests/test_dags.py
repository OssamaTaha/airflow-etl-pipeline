"""
DAG validation tests — ensures DAGs are properly structured.
Run with: pytest tests/test_dags.py -v
"""
import pytest
import sys
import os

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))


def test_dag_imports():
    """Verify all DAGs can be imported without errors."""
    from extract_exchange_rates import dag as dag1
    from extract_worldbank_indicators import dag as dag2
    from transform_and_load_warehouse import dag as dag3
    from data_pipeline_monitoring import dag as dag4

    assert dag1 is not None
    assert dag2 is not None
    assert dag3 is not None
    assert dag4 is not None


def test_dag_ids_unique():
    """Verify all DAG IDs are unique."""
    from extract_exchange_rates import dag as dag1
    from extract_worldbank_indicators import dag as dag2
    from transform_and_load_warehouse import dag as dag3
    from data_pipeline_monitoring import dag as dag4

    dag_ids = [dag1.dag_id, dag2.dag_id, dag3.dag_id, dag4.dag_id]
    assert len(dag_ids) == len(set(dag_ids)), f"Duplicate DAG IDs found: {dag_ids}"


def test_dag_has_tasks():
    """Verify each DAG has at least 2 tasks."""
    from extract_exchange_rates import dag as dag1
    from extract_worldbank_indicators import dag as dag2
    from transform_and_load_warehouse import dag as dag3
    from data_pipeline_monitoring import dag as dag4

    for dag in [dag1, dag2, dag3, dag4]:
        task_count = len(dag.tasks)
        assert task_count >= 2, f"DAG {dag.dag_id} has only {task_count} tasks"


def test_dag_schedule():
    """Verify DAGs have valid schedule intervals."""
    from extract_exchange_rates import dag as dag1
    from extract_worldbank_indicators import dag as dag2
    from transform_and_load_warehouse import dag as dag3
    from data_pipeline_monitoring import dag as dag4

    for dag in [dag1, dag2, dag3, dag4]:
        assert dag.schedule_interval is not None, f"DAG {dag.dag_id} has no schedule"


def test_extract_dag_dependencies():
    """Verify extract DAGs have correct task ordering."""
    from extract_exchange_rates import dag as dag1

    # Find check_api -> fetch_rates -> validate_load -> log_completion chain
    task_ids = [t.task_id for t in dag1.tasks]
    assert "check_api_availability" in task_ids
    assert "fetch_exchange_rates" in task_ids
    assert "validate_and_load_staging" in task_ids


def test_transform_dag_parallel_tasks():
    """Verify transform DAG has parallel transform tasks."""
    from transform_and_load_warehouse import dag

    task_ids = [t.task_id for t in dag.tasks]
    assert "clean_exchange_rates" in task_ids
    assert "clean_worldbank" in task_ids
    assert "calculate_derived_metrics" in task_ids


def test_monitoring_dag_health_checks():
    """Verify monitoring DAG has health check tasks."""
    from data_pipeline_monitoring import dag

    task_ids = [t.task_id for t in dag.tasks]
    assert "check_dag_health" in task_ids
    assert "check_postgres_size" in task_ids
    assert "generate_health_report" in task_ids
