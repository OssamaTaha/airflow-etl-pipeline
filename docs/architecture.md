# Architecture — Airflow ETL Pipeline

## Overview

This pipeline demonstrates production-grade ETL patterns using Apache Airflow as the orchestration engine.

## Data Flow

```
External APIs → Extract DAGs → staging.* tables → Transform DAG → production.* tables → Dashboard
                                                    ↓
                                              monitoring.* tables ← Monitoring DAG
```

## Schema Design

### Staging Layer
- `staging.exchange_rates` — Raw JSON + extracted fields from currency API
- `staging.worldbank_indicators` — Individual indicator records per country/year
- `staging.api_users` — JSONPlaceholder user data
- `staging.csv_ingestions` — File-based data ingestion

### Production Layer
- `production.exchange_rates` — Deduplicated rates with UPSERT logic
- `production.country_indicators` — Pivoted: one row per country/year with columns for each indicator
- `production.derived_metrics` — Calculated values (GDP per capita, etc.)

### Monitoring Layer
- `monitoring.pipeline_runs` — DAG execution history
- `monitoring.table_stats` — Row counts and sizes over time
- `monitoring.quality_checks` — Automated data quality validation results

## DAG Design Patterns

### 1. Extract → Transform → Load (ETL)
Classic three-stage pipeline with staging as an intermediate layer. This pattern ensures:
- Raw data is preserved for debugging
- Transformations are idempotent (can re-run safely)
- Production tables are never in an inconsistent state

### 2. Dynamic Task Mapping
The World Bank DAG uses Airflow's `.expand()` to create one parallel task per indicator:
```python
fetch_tasks = fetch_indicators_task.expand(indicator=INDICATOR_CODES)
```
This demonstrates handling variable-length inputs in a DAG.

### 3. DAG Dependencies
The transform DAG is scheduled after extract DAGs (07:00 vs 06:00). In production, use Airflow's `ExternalTaskSensor` or Trigger DAGs API for explicit dependency management.

### 4. Operational Monitoring
A dedicated monitoring DAG runs every 6 hours to:
- Track pipeline run history and durations
- Monitor table growth
- Run data quality checks
- Feed a Streamlit dashboard

## Technology Choices

| Choice | Reasoning |
|--------|-----------|
| CeleryExecutor | Production-grade, supports distributed workers |
| PostgreSQL | Industry standard for DE, shows SQL skills |
| Staging pattern | Idempotent, debuggable, auditable |
| Streamlit | Fast dashboard development in Python |
| Docker Compose | One-command deployment, portable |

## Scaling Considerations

- **CeleryExecutor** allows adding worker nodes for parallel task execution
- **Partitioning**: Staging tables should be partitioned by extraction_timestamp at scale
- **Airflow Pools**: Limit concurrent API calls to avoid rate limiting
- **Data retention**: Add TTL/purge jobs for staging tables older than N days
