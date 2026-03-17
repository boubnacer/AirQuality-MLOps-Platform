import os
import psycopg2
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class NewDataSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, min_new_records: int = 100, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.min_new_records = min_new_records

    def poke(self, context):
        last_run = context["prev_data_interval_end_success"]
        if last_run is None:
            self.log.info("No previous run, proceeding.")
            return True

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "airquality"),
            user=os.getenv("POSTGRES_USER", "airquality"),
            password=os.getenv("POSTGRES_PASSWORD", "airquality123"),
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM measurements WHERE created_at > %s",
                    (last_run,),
                )
                count = cur.fetchone()[0]
            self.log.info(f"New records since last run: {count} (need {self.min_new_records})")
            return count >= self.min_new_records
        finally:
            conn.close()