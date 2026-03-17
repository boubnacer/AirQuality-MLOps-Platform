#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== Air Quality Platform Setup ==="

# Create required directories
mkdir -p logs plugins mlflow-artifacts

# Copy .env.example to .env if not exists
if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env from .env.example — please fill in credentials"
fi

# Set AIRFLOW_UID
AIRFLOW_UID=$(id -u)
if grep -q "^AIRFLOW_UID=" .env; then
    sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$AIRFLOW_UID/" .env && rm -f .env.bak
else
    echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
fi

echo "AIRFLOW_UID set to $AIRFLOW_UID"

# Create Python virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "Virtual environment ready"

echo ""
echo "=== Setup complete ==="
echo "Next: docker compose up --build -d"
echo "Then: bash scripts/create_kafka_topics.sh"