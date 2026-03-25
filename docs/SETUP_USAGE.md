# Setup and Usage Guide

This guide covers everything needed to get the platform running from a fresh clone, verify all services, and walk through the full end-to-end data flow.

---

## Prerequisites

Install the following before starting:

- Docker Desktop (with at least 6 GB RAM allocated in Docker settings)
- Git

Docker Desktop download: https://www.docker.com/products/docker-desktop

Verify Docker is running:

```bash
docker --version
docker compose version
```

---

## 1. Clone the Repository

```bash
git https://github.com/boubnacer/AirQuality-MLOps-Platform.git
cd AirQuality-MLOps-Platform
```

---

## 2. Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Open `.env` and fill in any values you want to change. The defaults work out of the box for local development. The only optional values that require external accounts are:

- `OPENAQ_API_KEY` — only needed if you plan to run the historical backfill DAG to pull real pollution data from OpenAQ. Leave as-is if using the simulator.
- `SMTP_HOST`, `SMTP_USER`, `SMTP_PASSWORD` — only needed for email alerting on DAG failures.
- `SLACK_WEBHOOK_URL` — only needed for Slack alerting on DAG failures.

Set your system user ID for Airflow (required on Linux/Mac, prevents permission errors):

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

---

## 3. Create Required Directories

```bash
mkdir -p logs plugins mlflow-artifacts
```

---

## 4. Build and Start All Services

```bash
docker compose up --build -d
```

This starts 13 services. The first build takes 5-10 minutes as images are downloaded and dependencies are installed. Subsequent starts are much faster.

Wait for services to become healthy:

```bash
docker compose ps
```

All services should show `healthy` or `Up` status. The `airflow-init` service will exit with code 0 — that is expected.

---

## 5. Create Kafka Topics

Run this once after the stack is up:

```bash
bash scripts/create_kafka_topics.sh
```

This creates the three required topics: `raw_data`, `processed_data`, `anomalies`.

---

## 6. Service URLs

Once everything is running, the services are available at:

| Service | URL | Credentials |
|---|---|---|
| Airflow (orchestration) | http://localhost:8080 | admin / admin123 |
| MLflow (model tracking) | http://localhost:5050 | none |
| Metabase (dashboards) | http://localhost:3000 | set up on first visit |
| Prediction API | http://localhost:8000 | none |
| Prediction API docs | http://localhost:8000/docs | none |
| Catalog API | http://localhost:8001 | none |
| Catalog API docs | http://localhost:8001/docs | none |
| Kafka UI | http://localhost:8090 | none |
| PostgreSQL | localhost:5432 | airquality / airquality123 |
| MongoDB | localhost:27017 | none |

---

## 7. Verify Each Service Is Working

### Airflow

Open http://localhost:8080 and log in with `admin` / `admin123`.

You should see two DAGs listed:
- `daily_air_quality_pipeline` — the main pipeline
- `historical_backfill` — optional, for real data from OpenAQ

If a DAG shows an import error (red banner), check the scheduler logs:

```bash
docker logs airflow-scheduler --tail=50
```

### MLflow

Open http://localhost:5050. You should see the MLflow Experiments page. No login required.

Verify via terminal:

```bash
curl http://localhost:5050/health
```

Expected response: `{"status":"OK"}`

### Kafka Topics

Open http://localhost:8090 and click the Topics tab. Verify these three topics exist:

- `raw_data`
- `processed_data`
- `anomalies`

If they are missing, re-run:

```bash
bash scripts/create_kafka_topics.sh
```

### Prediction API

```bash
curl http://localhost:8000/health
```

Expected: JSON with `status`, `classifier_name`, `forecaster_name` fields.

### Database (PostgreSQL)

```bash
docker exec postgres psql -U airquality -d airquality -c "\dt"
```

Expected: list of tables including `measurements`, `anomalies`, `daily_summaries`, `aggregates`, `etl_heartbeat`.

---

## 8. Metabase First-Time Setup

Metabase takes about 2 minutes to initialise on first launch.

1. Open http://localhost:3000
2. Select your language and click "Let's get started"
3. Create an admin account (any email and password you choose)
4. On the "Add your data" step, select PostgreSQL and enter:
   - Host: `postgres`
   - Port: `5432`
   - Database name: `airquality`
   - Username: `airquality`
   - Password: `airquality123`
5. Click "Connect database" then finish setup

You can now browse tables and create dashboards. Use the pre-built SQL queries in `dashboards/sql/` as a starting point.

---

## 9. End-to-End Usage Walkthrough

Follow these steps in order to see the full data flow working.

### Step 1 — Confirm data is streaming

The simulator starts automatically and emits readings from 50 sensors every second. Check it is running:

```bash
docker logs simulator --tail=10
```

You should see lines like `Starting sensor CAP_001 in London`.

Check the ETL consumer is processing messages:

```bash
docker logs etl-consumer --tail=10
```

You should see lines like `Processed 5000 messages total` and `Flushed 1min aggregate for CAP_001`.

### Step 2 — Verify data is in the database

```bash
docker exec postgres psql -U airquality -d airquality \
  -c "SELECT COUNT(*), MAX(timestamp) FROM measurements;"
```

The count should be in the thousands and the max timestamp should be within the last few seconds.

```bash
docker exec postgres psql -U airquality -d airquality \
  -c "SELECT COUNT(*) FROM anomalies;"
```

Anomalies are written in real time when sensor readings exceed thresholds.

### Step 3 — Test the prediction API (before model training)

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_id": "sensor_001",
    "timestamp": "2024-01-15T14:30:00",
    "location": {"zone": "urban_background", "lat": 48.8, "lon": 2.3},
    "measurements": {
      "pm2_5": 45.2,
      "pm10": 78.1,
      "no2": 52.3,
      "o3": 89.1,
      "co": 0.8,
      "so2": 12.1,
      "temperature": 22.5,
      "humidity": 65.0
    }
  }'
```

Before model training, `anomaly_probability`, `is_anomaly`, `pm2_5_forecast`, and `no2_forecast` will all be `null` and `model_name` will be `"none"`. This is expected.

### Step 4 — Trigger the daily pipeline in Airflow

1. Open http://localhost:8080
2. Find `daily_air_quality_pipeline`
3. If the toggle shows it is paused (grey), click the toggle to unpause it
4. Click the play button on the right side of the row
5. Select "Trigger DAG" and confirm
6. Click the DAG name, then click the "Graph" tab to watch task execution

The pipeline runs these tasks in order:

```
wait_for_new_data + wait_for_streaming
        |
  extract_sensor_data
        |
  validate_data_quality
        |
  compute_daily_summaries
        |
  train_classifier + train_forecaster + train_clustering  (parallel)
        |
  evaluate_model
        |
  branch_deploy
     /         \
deploy_model   skip_deploy
     \         /
   update_catalog
```

Each task circle turns dark green when it succeeds. The full run takes approximately 5-10 minutes.

If a task fails (turns red), click it and then click "Logs" to see the error.

### Step 5 — Verify models in MLflow

Once training tasks complete, open http://localhost:5050 and go to the Experiments tab.

You should see three experiments:
- `anomaly-classification`
- `pollution-forecasting`
- `sensor-clustering`

Click any experiment to see the training runs, metrics (rmse, mae, r2), and logged model artifacts.

Go to the Model Registry tab. You should see:
- `air-quality-anomaly-classifier`
- `air-quality-pollution-forecaster`
- `air-quality-sensor-clustering`

Models promoted to Staging or Production will be visible here.

### Step 6 — Hot-reload models into the prediction API

After training, tell the API to load the new models from MLflow without restarting:

```bash
curl -X POST http://localhost:8000/reload
```

Expected response: `{"status":"reloaded"}`

### Step 7 — Test the prediction API (after model training)

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_id": "sensor_001",
    "timestamp": "2024-01-15T14:30:00",
    "location": {"zone": "urban_background", "lat": 48.8, "lon": 2.3},
    "measurements": {
      "pm2_5": 45.2,
      "pm10": 78.1,
      "no2": 52.3,
      "o3": 89.1,
      "co": 0.8,
      "so2": 12.1,
      "temperature": 22.5,
      "humidity": 65.0
    }
  }'
```

Now you should see real values:

```json
{
  "sensor_id": "sensor_001",
  "timestamp": "2024-01-15T14:30:00",
  "anomaly_probability": 0.82,
  "is_anomaly": true,
  "pm2_5_forecast": 48.3,
  "no2_forecast": 55.1,
  "model_name": "air-quality-anomaly-classifier",
  "model_version": "1"
}
```

### Step 8 — Check daily summaries were computed

```bash
docker exec postgres psql -U airquality -d airquality \
  -c "SELECT sensor_id, summary_date, pm2_5_avg, anomaly_count FROM daily_summaries LIMIT 10;"
```

### Step 9 — Browse dashboards in Metabase

Open http://localhost:3000 and use the SQL editor (click the pencil icon or "New Question") to run queries from `dashboards/sql/`:

- `realtime_monitor.sql` — live sensor readings and anomaly flags
- `model_performance.sql` — model metrics over time
- `dataops_metrics.sql` — pipeline health and data quality metrics

### Step 10 — Check Kafka message flow

Open http://localhost:8090 and go to the Topics tab. Click `raw_data` to see messages being produced by the simulator in real time. Click `anomalies` to see anomaly events published by the ETL consumer.

---

## 10. Stopping the Platform

To stop all services without removing data:

```bash
docker compose down
```

To stop and remove all data volumes (full reset):

```bash
docker compose down -v
```

To restart after stopping:

```bash
docker compose up -d
```

---

## 11. Checking Service Health

```bash
# All containers status
docker compose ps

# Logs for a specific service
docker logs <service-name> --tail=50

# Follow logs in real time
docker logs <service-name> -f
```

Service names: `simulator`, `etl-consumer`, `airflow-webserver`, `airflow-scheduler`, `mlflow`, `prediction-api`, `catalog-api`, `kafka`, `kafka-ui`, `postgres`, `mongodb`, `metabase`, `zookeeper`
