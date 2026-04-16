#!/bin/bash
set -e

echo "======================================"
echo "  Airflow ETL Pipeline — First Setup"
echo "======================================"

# Create .env file if not exists
if [ ! -f .env ]; then
    echo "AIRFLOW_UID=$(id -u)" > .env
    echo "Created .env file with AIRFLOW_UID=$(id -u)"
fi

# Build and start
echo ""
echo "Building Docker images..."
docker compose build

echo ""
echo "Starting services..."
docker compose up -d

echo ""
echo "Waiting for Airflow to initialize (30s)..."
sleep 30

echo ""
echo "======================================"
echo "  Setup Complete!"
echo "======================================"
echo ""
echo "  Airflow UI: http://localhost:8080"
echo "  Login:      airflow / airflow"
echo ""
echo "  Dashboard:  http://localhost:8501"
echo ""
echo "  First DAG run will happen at next schedule."
echo "  To trigger manually:"
echo "    docker compose exec airflow-webserver airflow dags trigger extract_exchange_rates"
echo ""
