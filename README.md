# Real-Time Air Quality Anomaly Detection Platform

A production-grade DataOps/MLOps platform that ingests real-time air quality sensor data, detects anomalies, forecasts pollution levels, and serves predictions through a REST API — all orchestrated with Apache Airflow and tracked with MLflow.

---

## What It Does

- Simulates 50 air quality sensors across multiple cities emitting readings every second
- Streams sensor data through Apache Kafka into a PostgreSQL data warehouse
- Detects anomalies in real time using rule-based thresholds and ML classification
- Trains and evaluates three ML models daily via Airflow-orchestrated pipelines
- Promotes the best-performing model to production through MLflow Model Registry
- Serves live anomaly scores and pollution forecasts through a FastAPI prediction endpoint
- Visualizes all metrics, trends, and anomalies in Metabase dashboards
- Maintains a data catalog with quality metrics tracked per pipeline run

---

## Features

- Real-time streaming pipeline processing 500+ messages per second
- Kafka-based decoupled producer/consumer architecture with three topics: `raw_data`, `processed_data`, `anomalies`
- Sliding window aggregation (1-minute, 5-minute intervals) over sensor streams
- Three ML models trained and versioned automatically:
  - Anomaly classifier (Random Forest + XGBoost)
  - Pollution forecaster (Ridge Regression + XGBoost) for PM2.5 and NO2
  - Sensor clustering (KMeans) for zone-level pattern detection
- Champion/challenger model evaluation — only promotes a new model if it outperforms production
- FastAPI prediction API with async model loading and hot-reload endpoint
- Data catalog API exposing table-level metadata, row counts, and freshness metrics
- Daily data quality validation with null-rate checks and automatic failure alerts
- Slack and email alerting on DAG failures
- Metabase connected to PostgreSQL for live dashboards

---

## Tech Stack

| Layer | Tools |
|---|---|
| Streaming | Apache Kafka, Zookeeper |
| Orchestration | Apache Airflow 2.x |
| ML Tracking | MLflow (Tracking Server + Model Registry) |
| ML Models | scikit-learn, XGBoost |
| API | FastAPI, Uvicorn |
| Database | PostgreSQL 15, MongoDB 7 |
| Dashboards | Metabase |
| Monitoring | Kafka UI |
| Containerisation | Docker, Docker Compose |
| Language | Python 3.11 |

---

## Project Structure

```
air-quality-platform/
├── api/                        # FastAPI prediction service
│   ├── main.py                 # /predict, /health, /reload endpoints
│   ├── model_loader.py         # MLflow model loading logic
│   └── schemas.py              # Request/response Pydantic models
├── catalog/                    # Data catalog API
│   ├── app.py                  # FastAPI catalog endpoints
│   └── ingest_metadata.py      # Metadata ingestion from PostgreSQL
├── dags/                       # Airflow DAGs and custom operators
│   ├── daily_air_quality_pipeline.py   # Main daily pipeline DAG
│   ├── historical_backfill.py          # OpenAQ historical data DAG
│   ├── operators/
│   │   └── mlflow_train_operator.py    # Custom Airflow operator for ML training
│   └── sensors/
│       ├── new_data_sensor.py          # Waits for minimum new records
│       └── streaming_done_sensor.py    # Waits for ETL consumer to stabilise
├── dashboards/
│   └── sql/                    # Pre-built SQL queries for Metabase
│       ├── realtime_monitor.sql
│       ├── model_performance.sql
│       └── dataops_metrics.sql
├── docs/
│   └── SETUP_USAGE.md          # Complete setup and usage guide
├── ml/                         # ML training and evaluation code
│   ├── train.py                # Entry point for all model training
│   ├── evaluate.py             # Champion/challenger evaluation logic
│   ├── utils.py                # MLflow run logging helpers
│   ├── data/
│   │   └── feature_engineering.py     # Feature matrix construction
│   └── models/
│       ├── anomaly_classifier.py
│       ├── pollution_forecaster.py
│       └── sensor_clustering.py
├── scripts/
│   ├── init_postgres.sql       # Database schema and grants
│   ├── seed_locations.sql      # Sensor location seed data
│   └── create_kafka_topics.sh  # Kafka topic creation script
├── streaming/
│   ├── producer/               # Sensor simulator (Kafka producer)
│   │   ├── simulator.py
│   │   └── config.py
│   └── consumer/               # ETL consumer (Kafka to PostgreSQL)
│       ├── etl_consumer.py
│       ├── anomaly_detector.py
│       ├── window_aggregator.py
│       ├── db.py
│       └── schema.py
├── docker-compose.yml          # Full stack orchestration (13 services)
├── Dockerfile.airflow          # Custom Airflow image with ML dependencies
├── requirements.txt            # Python dependencies
├── setup.sh                    # Local environment bootstrap script
└── .env.example                # Environment variable template
```

---

## Setup and Usage

See [docs/SETUP_USAGE.md](docs/SETUP_USAGE.md) for complete setup instructions, service URLs, and end-to-end usage walkthrough.
