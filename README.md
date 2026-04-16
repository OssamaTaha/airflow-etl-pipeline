# Airflow ETL Pipeline — Multi-Source Data Orchestration

Production-grade ETL pipeline built with Apache Airflow, demonstrating DAG design, task dependencies, dynamic task mapping, and automated monitoring.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    AIRFLOW WEB UI                        │
│              localhost:8080 (monitoring)                 │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                   AIRFLOW SCHEDULER                       │
│         Triggers DAGs on schedule / manual               │
└──────────────────────────┬──────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  DAG 1   │    │  DAG 2   │    │  DAG 3   │
   │ Exchange │    │  World   │    │Transform │
   │  Rates   │    │  Bank    │    │ & Load   │
   └────┬─────┘    └────┬─────┘    └────┬─────┘
        │               │               │
        └───────────────┼───────────────┘
                        ▼
              ┌──────────────────┐
              │   PostgreSQL     │
              │  (Target DW)     │
              └──────────────────┘
                        │
              ┌──────────────────┐
              │   Streamlit UI   │
              │  (Dashboard)     │
              └──────────────────┘
```

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `extract_exchange_rates` | Daily 06:00 UTC | Fetches currency rates from open.er-api.com (USD, EUR, EGP bases) |
| `extract_worldbank_indicators` | Weekly Sun 02:00 UTC | Extracts GDP/population/inflation for Middle East countries |
| `transform_and_load_warehouse` | Daily 07:00 UTC | Cleans staging data, pivots indicators, calculates derived metrics |
| `data_pipeline_monitoring` | Every 6 hours | Health checks, table size monitoring, quality reports |

## Quick Start

```bash
# Clone and start
git clone https://github.com/OssamaTaha/airflow-etl-pipeline.git
cd airflow-etl-pipeline

# One-command deploy
docker compose up -d

# Wait ~30s for initialization, then open:
# Airflow UI: http://localhost:8080 (airflow/airflow)
# Dashboard:  http://localhost:8501
```

## Data Sources (Free, No API Keys)

- **Exchange Rates** — [open.er-api.com](https://open.er-api.com) (150+ currencies, daily)
- **World Bank** — [data.worldbank.org](https://data.worldbank.org) (GDP, population, unemployment, inflation)
- **JSONPlaceholder** — [jsonplaceholder.typicode.com](https://jsonplaceholder.typicode.com) (test REST API)

## Key Technical Features

- **DAG Dependencies**: Transform DAG waits for extract DAGs to complete
- **Dynamic Task Mapping**: World Bank DAG uses `.expand()` for parallel per-indicator extraction
- **XCom**: Inter-task data passing between extraction and transformation
- **Retry Logic**: Exponential backoff with configurable max retries
- **Staging Pattern**: Raw → Staging → Production data flow
- **Idempotent Transforms**: UPSERT with `ON CONFLICT` ensures re-runnability
- **Monitoring DAG**: Automated health checks with quality reports
- **Docker Compose**: Full stack (Airflow + Postgres + Redis + Celery) in one command

## Project Structure

```
airflow-etl-pipeline/
├── docker-compose.yaml          # Full stack orchestration
├── Dockerfile                   # Custom Airflow image
├── requirements.txt             # Python dependencies
├── dags/
│   ├── extract_exchange_rates.py
│   ├── extract_worldbank_indicators.py
│   ├── transform_and_load_warehouse.py
│   ├── data_pipeline_monitoring.py
│   └── utils/
│       ├── api_clients.py       # API wrapper classes
│       ├── validators.py        # Data validation helpers
│       └── db_helpers.py        # PostgreSQL utilities
├── sql/
│   ├── init.sql                 # Schema creation
│   └── transforms.sql           # Transform queries
├── dashboard/
│   └── app.py                   # Streamlit monitoring UI
├── tests/
│   ├── test_dags.py             # DAG structure validation
│   ├── test_extract.py          # API + validator tests
│   └── test_transform.py        # Transform logic tests
├── config/
│   └── connections.yaml         # Airflow connection definitions
├── .github/workflows/
│   └── ci.yml                   # Lint + test on PR
└── docs/
    └── architecture.md          # Detailed architecture docs
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test module
pytest tests/test_dags.py -v
pytest tests/test_extract.py -v
pytest tests/test_transform.py -v
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Orchestration | Apache Airflow 2.9+ | DAG scheduling and monitoring |
| Containers | Docker Compose | Portable deployment |
| Database | PostgreSQL 15 | Metadata + target data warehouse |
| Backend | Python 3.11+ | Core language |
| Dashboard | Streamlit | Monitoring UI |
| Testing | pytest | Unit + integration tests |
| CI/CD | GitHub Actions | Automated testing |

## License

MIT

---

**Built by [Ossama Taha](https://github.com/OssamaTaha)** — Data Engineer
