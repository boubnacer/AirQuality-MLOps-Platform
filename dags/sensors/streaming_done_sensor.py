import os
import psycopg2
from datetime import date, timedelta
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class StreamingDoneSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, max_age_minutes: int = 10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_age_minutes = max_age_minutes

    def poke(self, context):
        today = date.today()
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
                    """
                    SELECT last_batch_ts, status FROM etl_heartbeat
                    WHERE pipeline_date = %s
                    """,
                    (today,),
                )
                row = cur.fetchone()
            if row is None:
                self.log.info("No heartbeat for today yet.")
                return False
            last_batch_ts, status = row
            from datetime import datetime, timezone
            age = (datetime.now(timezone.utc) - last_batch_ts).total_seconds() / 60
            self.log.info(f"Heartbeat age: {age:.1f} min, status: {status}")
            return age <= self.max_age_minutes and status == "running"
        finally:
            conn.close()