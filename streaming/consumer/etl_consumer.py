import json
import os
import psycopg2.extras
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from loguru import logger
from pymongo import MongoClient
from pydantic import ValidationError

from schema import SensorPayload
from db import insert_measurement, insert_anomaly, upsert_heartbeat
from anomaly_detector import check_anomalies
from window_aggregator import WindowAggregator
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://airquality:airquality123@localhost:27017/")

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "etl-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
}
producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "etl-anomaly-producer",
}

SEEN_KEYS = set()  # Simple dedup for in-memory; use Redis in production
MAX_SEEN = 100_000


def main():
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe(["raw_data"])
    aggregator = WindowAggregator()

    # MongoDB (optional — graceful fallback)
    mongo_col = None
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.server_info()
        mongo_col = mongo_client["airquality_raw"]["raw_messages"]
        logger.info("MongoDB connected")
    except Exception as e:
        logger.warning(f"MongoDB unavailable, skipping: {e}")

    batch_count = 0
    logger.info("ETL consumer started, waiting for messages...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            raw_value = msg.value()
            if raw_value is None:
                continue

            try:
                data = json.loads(raw_value)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON: {e}")
                continue

            # Schema validation
            try:
                payload = SensorPayload(**data)
            except ValidationError as e:
                logger.warning(f"Schema validation failed for {data.get('sensor_id')}: {e}")
                continue

            # Dedup check
            dedup_key = f"{payload.sensor_id}:{payload.timestamp.isoformat()}"
            if dedup_key in SEEN_KEYS:
                continue
            SEEN_KEYS.add(dedup_key)
            if len(SEEN_KEYS) > MAX_SEEN:
                SEEN_KEYS.clear()

            m = payload.measurements
            flat = {
                "sensor_id": payload.sensor_id,
                "timestamp": payload.timestamp,
                "pm2_5": m.pm2_5,
                "pm10": m.pm10,
                "no2": m.no2,
                "o3": m.o3,
                "co": m.co,
                "so2": m.so2,
                "temperature": m.temperature,
                "humidity": m.humidity,
                "battery_level": payload.battery_level,
            }

            # Insert to PostgreSQL
            insert_measurement(flat)

            # MongoDB raw store
            if mongo_col is not None:
                try:
                    mongo_col.insert_one(data)
                except Exception:
                    pass

            # Rule-based anomaly detection
            anomalies = check_anomalies(data, producer)
            for anom in anomalies:
                serializable_data = json.loads(json.dumps(data, default=str))
                anom_db = {**anom, "raw_payload": psycopg2.extras.Json(serializable_data)}
                insert_anomaly(anom_db)

            # Rolling window aggregation
            measurements_dict = {
                "pm2_5": m.pm2_5, "pm10": m.pm10, "no2": m.no2,
                "o3": m.o3, "co": m.co, "so2": m.so2,
            }
            aggregator.add(payload.sensor_id, payload.timestamp, measurements_dict)
            now = datetime.now(timezone.utc)
            aggregator.maybe_flush(payload.sensor_id, now)

            batch_count += 1
            if batch_count % 500 == 0:
                upsert_heartbeat(500)
                logger.info(f"Processed {batch_count} messages total")

    except KeyboardInterrupt:
        logger.info("Consumer shutting down")
    finally:
        consumer.close()
        upsert_heartbeat(batch_count % 500)


if __name__ == "__main__":
    main()
