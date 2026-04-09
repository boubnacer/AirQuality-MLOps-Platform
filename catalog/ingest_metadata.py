import os
import psycopg2
from datetime import date
from loguru import logger

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "airquality"),
    "user": os.getenv("POSTGRES_USER", "airquality"),
    "password": os.getenv("POSTGRES_PASSWORD", "airquality123"),
}

DATA_SOURCES = [
    {"name": "kafka_raw_data", "source_type": "kafka", "description": "Raw sensor readings from IoT simulator"},
    {"name": "kafka_anomalies", "source_type": "kafka", "description": "Rule-based anomaly events"},
    {"name": "postgres_airquality", "source_type": "postgresql", "description": "Processed air quality data"},
    {"name": "mlflow_registry", "source_type": "mlflow", "description": "ML model artifacts"},
]

LINEAGE = [
    ("kafka:raw_data", "etl_consumer", "schema_validation"),
    ("etl_consumer", "postgres:measurements", "insert"),
    ("etl_consumer", "kafka:anomalies", "rule_based_detection"),
    ("postgres:measurements", "postgres:aggregates", "rolling_window"),
    ("postgres:measurements", "postgres:daily_summaries", "daily_batch"),
    ("postgres:measurements", "mlflow:models", "model_training"),
    ("mlflow:models", "api:predict", "model_serving"),
    ("postgres:daily_summaries", "metabase:dashboards", "visualization"),
]

TABLE_META = {
    "measurements": "Raw pollutant readings from IoT sensors, one row per sensor per reading",
    "aggregates": "Rolling 1-min and 5-min window statistics per sensor",
    "anomalies": "Rule-based and ML-based anomaly events",
    "daily_summaries": "Daily aggregated statistics per sensor for dashboards",
    "sensors": "Static sensor registry with location metadata",
    "etl_heartbeat": "ETL pipeline health monitoring, updated every 500 records",
}


def _upsert_source(cur, name: str, source_type: str, description: str) -> int:
    cur.execute(
        """
        INSERT INTO catalog.data_sources (name, source_type, description)
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description
        RETURNING id
        """,
        (name, source_type, description),
    )
    return cur.fetchone()[0]


def _upsert_table(cur, source_id: int, table_name: str, description: str):
    cur.execute(
        """
        INSERT INTO catalog.tables (source_id, table_name, schema_name, description)
        VALUES (%s, %s, 'public', %s)
        ON CONFLICT DO NOTHING
        """,
        (source_id, table_name, description),
    )


def _upsert_lineage(cur, source_name: str, target_name: str, transformation: str):
    cur.execute(
        """
        INSERT INTO catalog.lineage_edges (source_name, target_name, transformation)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (source_name, target_name, transformation),
    )


def _compute_quality_metrics(cur, table_name: str, run_date: date):
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cur.fetchone()[0]

        cur.execute(f"SELECT MAX(created_at) FROM {table_name}")
        last_insert = cur.fetchone()[0]
        freshness = None
        if last_insert:
            from datetime import datetime, timezone
            freshness = int((datetime.now(timezone.utc) - last_insert).total_seconds() / 60)

        null_rate = 0.0
        if table_name == "measurements" and row_count > 0:
            cur.execute("SELECT COUNT(*) FROM measurements WHERE pm2_5 IS NULL")
            null_count = cur.fetchone()[0]
            null_rate = round(null_count / row_count, 4)

        cur.execute(
            """
            INSERT INTO catalog.quality_metrics
                (table_name, pipeline_date, row_count, null_rate, freshness_minutes)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (table_name, run_date, row_count, null_rate, freshness),
        )
    except Exception as e:
        logger.warning(f"quality_metrics for {table_name}: {e}")


def run():
    today = date.today()
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            pg_source_id = None
            for src in DATA_SOURCES:
                source_id = _upsert_source(cur, src["name"], src["source_type"], src["description"])
                if src["name"] == "postgres_airquality":
                    pg_source_id = source_id

            if pg_source_id:
                for table_name, description in TABLE_META.items():
                    _upsert_table(cur, pg_source_id, table_name, description)

            for source_name, target_name, transformation in LINEAGE:
                _upsert_lineage(cur, source_name, target_name, transformation)

            for table_name in TABLE_META:
                _compute_quality_metrics(cur, table_name, today)

        conn.commit()
        logger.info(f"Catalog updated for {today}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Catalog ingestion failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
