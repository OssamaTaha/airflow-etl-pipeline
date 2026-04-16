#!/bin/bash
set -e

echo "======================================"
echo "  Airflow ETL Pipeline — Clean Reset"
echo "======================================"

read -p "This will destroy all data and containers. Continue? (y/N) " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Stopping and removing containers..."
docker compose down -v

echo ""
echo "Removing dangling images..."
docker image prune -f

echo ""
echo "======================================"
echo "  Reset Complete!"
echo "======================================"
echo ""
echo "  Run ./scripts/setup.sh to start fresh."
echo ""
