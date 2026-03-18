from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import statistics
from loguru import logger
from db import insert_aggregate


class WindowAggregator:
    """Maintains in-memory rolling windows (1-min and 5-min) per sensor."""

    def __init__(self):
        self.windows = {
            "1min": defaultdict(lambda: deque()),
            "5min": defaultdict(lambda: deque()),
        }
        self.durations = {
            "1min": timedelta(minutes=1),
            "5min": timedelta(minutes=5),
        }
        self.last_flush = {
            "1min": defaultdict(lambda: datetime.now(timezone.utc)),
            "5min": defaultdict(lambda: datetime.now(timezone.utc)),
        }

    def add(self, sensor_id: str, timestamp: datetime, measurements: dict):
        for window_type, duration in self.durations.items():
            dq = self.windows[window_type][sensor_id]
            dq.append((timestamp, measurements))
            # evict old entries
            while dq and (timestamp - dq[0][0]) > duration:
                dq.popleft()

    def maybe_flush(self, sensor_id: str, now: datetime):
        for window_type, duration in self.durations.items():
            last = self.last_flush[window_type][sensor_id]
            if (now - last) >= duration:
                self._flush(sensor_id, window_type, now, duration)
                self.last_flush[window_type][sensor_id] = now

    def _flush(self, sensor_id: str, window_type: str, window_end: datetime, duration: timedelta):
        dq = self.windows[window_type][sensor_id]
        if not dq:
            return
        window_start = window_end - duration
        records = [m for ts, m in dq if ts >= window_start]
        if not records:
            return

        def safe_stat(fn, vals):
            clean = [v for v in vals if v is not None]
            return round(fn(clean), 4) if clean else None

        def extract(key):
            return [r.get(key) for r in records]

        agg = {
            "sensor_id": sensor_id,
            "window_start": window_start,
            "window_end": window_end,
            "window_type": window_type,
            "pm2_5_mean": safe_stat(statistics.mean, extract("pm2_5")),
            "pm2_5_std": safe_stat(statistics.stdev, extract("pm2_5")) if len([v for v in extract("pm2_5") if v]) > 1 else 0,
            "pm2_5_min": safe_stat(min, extract("pm2_5")),
            "pm2_5_max": safe_stat(max, extract("pm2_5")),
            "pm10_mean": safe_stat(statistics.mean, extract("pm10")),
            "pm10_std": safe_stat(statistics.stdev, extract("pm10")) if len([v for v in extract("pm10") if v]) > 1 else 0,
            "no2_mean": safe_stat(statistics.mean, extract("no2")),
            "no2_std": safe_stat(statistics.stdev, extract("no2")) if len([v for v in extract("no2") if v]) > 1 else 0,
            "o3_mean": safe_stat(statistics.mean, extract("o3")),
            "co_mean": safe_stat(statistics.mean, extract("co")),
            "so2_mean": safe_stat(statistics.mean, extract("so2")),
            "record_count": len(records),
        }
        insert_aggregate(agg)
        logger.debug(f"Flushed {window_type} aggregate for {sensor_id}: {len(records)} records")
