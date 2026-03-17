
#Lahcen Atti
import os
import sys
import requests
import psycopg2
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")
OPENAQ_BASE = "https://api.openaq.org/v3"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "airquality"),
    "user": os.getenv("POSTGRES_USER", "airquality"),
    "password": os.getenv("POSTGRES_PASSWORD", "airquality123"),
}

SENSOR_LOCATION_IDS = [
    3847229, 3847230, 3847231,
]

POLLUTANT_MAP = {
    "pm25": "pm2_5",
    "pm10": "pm10",
    "no2": "no2",
    "o3": "o3",
    "co": "co",
    "so2": "so2",
}


def _fetch_openaq_measurements(location_id: int, date_from: str, date_to: str) -> list:
    headers = {"X-API-Key": OPENAQ_API_KEY} if OPENAQ_API_KEY else {}
    params = {
        "locations_id": location_id,
        "date_from": date_from,
        "date_to": date_to,
        "limit": 1000,
        "page": 1,
    }
    rows = []
    while True:
        resp = requests.get(f"{OPENAQ_BASE}/measurements", headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            import time; time.sleep(60)
            continue
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        rows.extend(results)
        if len(results) < params["limit"]:
            break
        params["page"] += 1
    return rows


def _ensure_sensor(conn, sensor_id: str, location_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM sensors WHERE sensor_id = %s", (sensor_id,))
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO sensors (sensor_id, lat, lon, city, zone)
                VALUES (%s, 0, 0, 'OpenAQ', 'unknown')
                ON CONFLICT DO NOTHING
                """,
                (sensor_id,),
            )
    conn.commit()


def backfill_date(**context):
    run_date = context["ds"]
    date_from = f"{run_date}T00:00:00Z"
    date_to = f"{run_date}T23:59:59Z"

    conn = psycopg2.connect(**DB_CONFIG)
    total = 0

    for location_id in SENSOR_LOCATION_IDS:
        sensor_id = f"OAQ_{location_id}"
        _ensure_sensor(conn, sensor_id, location_id)

        try:
            rows = _fetch_openaq_measurements(location_id, date_from, date_to)
        except Exception as e:
            print(f"Failed to fetch location {location_id}: {e}")
            continue

        grouped = {}
        for r in rows:
            ts = r.get("date", {}).get("utc", "")
            if not ts:
                continue
            if ts not in grouped:
                grouped[ts] = {p: None for p in POLLUTANT_MAP.values()}
            param = r.get("parameter", "")
            db_col = POLLUTANT_MAP.get(param)
            if db_col:
                grouped[ts][db_col] = r.get("value")

        with conn.cursor() as cur:
            for ts, measurements in grouped.items():
                cur.execute(
                    """
                    INSERT INTO measurements
                        (sensor_id, timestamp, pm2_5, pm10, no2, o3, co, so2)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        sensor_id, ts,
                        measurements["pm2_5"], measurements["pm10"],
                        measurements["no2"], measurements["o3"],
                        measurements["co"], measurements["so2"],
                    ),
                )
                total += 1
        conn.commit()

    conn.close()
    print(f"Backfilled {total} records for {run_date}")
    return total


with DAG(
    dag_id="historical_backfill",
    default_args=DEFAULT_ARGS,
    description="Backfill historical air quality data from OpenAQ API. ",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 3, 31),
    catchup=True,
    max_active_runs=3,
    tags=["air-quality", "backfill"],
    params={
        "location_ids": SENSOR_LOCATION_IDS,
    },
) as dag:

    backfill = PythonOperator(
        task_id="backfill_date",
        python_callable=backfill_date,
    )