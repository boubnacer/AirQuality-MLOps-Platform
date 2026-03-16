import os
import psycopg2
import psycopg2.extras
from loguru import logger
from datetime import date, datetime, timezone

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "airquality"),
    "user": os.getenv("POSTGRES_USER", "airquality"),
    "password": os.getenv("POSTGRES_PASSWORD", "airquality123"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def insert_measurement(payload: dict) -> bool:
    sql = """
        INSERT INTO measurements
            (sensor_id, timestamp, pm2_5, pm10, no2, o3, co, so2, temperature, humidity, battery_level)
        VALUES
            (%(sensor_id)s, %(timestamp)s, %(pm2_5)s, %(pm10)s, %(no2)s, %(o3)s,
             %(co)s, %(so2)s, %(temperature)s, %(humidity)s, %(battery_level)s)
        ON CONFLICT (sensor_id, timestamp) DO NOTHING
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
        return True
    except Exception as e:
        logger.error(f"insert_measurement error: {e}")
        return False


def insert_anomaly(anomaly: dict) -> bool:
    sql = """
        INSERT INTO anomalies
            (sensor_id, timestamp, anomaly_type, severity, pollutant, value, threshold, raw_payload)
        VALUES
            (%(sensor_id)s, %(timestamp)s, %(anomaly_type)s, %(severity)s,
             %(pollutant)s, %(value)s, %(threshold)s, %(raw_payload)s)
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, anomaly)
        return True
    except Exception as e:
        logger.error(f"insert_anomaly error: {e}")
        return False


def upsert_heartbeat(records_processed: int):
    sql = """
        INSERT INTO etl_heartbeat (pipeline_date, last_batch_ts, records_processed, status)
        VALUES (%s, %s, %s, 'running')
        ON CONFLICT (pipeline_date) DO UPDATE
            SET last_batch_ts = EXCLUDED.last_batch_ts,
                records_processed = etl_heartbeat.records_processed + EXCLUDED.records_processed,
                status = 'running',
                updated_at = NOW()
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (date.today(), datetime.now(timezone.utc), records_processed))
    except Exception as e:
        logger.error(f"upsert_heartbeat error: {e}")


def insert_aggregate(agg: dict):
    sql = """
        INSERT INTO aggregates
            (sensor_id, window_start, window_end, window_type,
             pm2_5_mean, pm2_5_std, pm2_5_min, pm2_5_max,
             pm10_mean, pm10_std, no2_mean, no2_std,
             o3_mean, co_mean, so2_mean, record_count)
        VALUES
            (%(sensor_id)s, %(window_start)s, %(window_end)s, %(window_type)s,
             %(pm2_5_mean)s, %(pm2_5_std)s, %(pm2_5_min)s, %(pm2_5_max)s,
             %(pm10_mean)s, %(pm10_std)s, %(no2_mean)s, %(no2_std)s,
             %(o3_mean)s, %(co_mean)s, %(so2_mean)s, %(record_count)s)
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, agg)
    except Exception as e:
        logger.error(f"insert_aggregate error: {e}")
