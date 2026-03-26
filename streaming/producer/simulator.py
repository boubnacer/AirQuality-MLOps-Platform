import json
import time
import random
import threading
from datetime import datetime, timezone
from confluent_kafka import Producer
from loguru import logger
from config import KAFKA_BOOTSTRAP_SERVERS, SENSOR_COUNT, EMISSION_INTERVAL_SECONDS, KAFKA_TOPIC_RAW, SENSOR_LOCATIONS

# Anomaly thresholds
THRESHOLDS = {
    "pm2_5": {"warning": 25, "critical": 75},
    "pm10": {"warning": 50, "critical": 150},
    "no2": {"warning": 100, "critical": 200},
    "o3": {"warning": 120, "critical": 180},
    "co": {"warning": 4, "critical": 10},
    "so2": {"warning": 125, "critical": 350},
}

# Base values by zone type
BASE_VALUES = {
    "urban_background": {"pm2_5": 15, "pm10": 25, "no2": 40, "o3": 60, "co": 0.5, "so2": 8},
    "traffic":          {"pm2_5": 25, "pm10": 45, "no2": 80, "o3": 50, "co": 1.2, "so2": 15},
    "industrial":       {"pm2_5": 35, "pm10": 65, "no2": 90, "o3": 40, "co": 2.0, "so2": 40},
    "residential":      {"pm2_5": 10, "pm10": 18, "no2": 25, "o3": 70, "co": 0.3, "so2": 5},
}


def generate_reading(sensor: dict) -> dict:
    zone = sensor["zone"]
    base = BASE_VALUES.get(zone, BASE_VALUES["urban_background"])
    spike = random.random() < 0.05  # 5% chance of anomaly spike

    def noisy(val, noise_pct=0.15):
        return max(0, val * (1 + random.gauss(0, noise_pct)))

    def spike_val(val, multiplier=4):
        return val * multiplier if spike else noisy(val)

    return {
        "sensor_id": sensor["sensor_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": {
            "lat": sensor["lat"],
            "lon": sensor["lon"],
            "city": sensor["city"],
            "zone": sensor["zone"],
        },
        "measurements": {
            "pm2_5": round(spike_val(base["pm2_5"]), 2),
            "pm10": round(spike_val(base["pm10"]), 2),
            "no2": round(spike_val(base["no2"]), 2),
            "o3": round(noisy(base["o3"]), 2),
            "co": round(spike_val(base["co"]), 3),
            "so2": round(noisy(base["so2"]), 2),
            "temperature": round(random.uniform(5, 35), 1),
            "humidity": round(random.uniform(30, 90), 1),
        },
        "battery_level": random.randint(20, 100),
        "status": random.choices(
            ["active", "maintenance", "offline"], weights=[95, 3, 2]
        )[0],
    }


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for {msg.topic()}: {err}")


def sensor_worker(sensor: dict, producer: Producer, interval: float):
    logger.info(f"Starting sensor {sensor['sensor_id']} in {sensor['city']}")
    while True:
        try:
            reading = generate_reading(sensor)
            producer.produce(
                KAFKA_TOPIC_RAW,
                key=sensor["sensor_id"],
                value=json.dumps(reading),
                callback=delivery_report,
            )
            producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing for {sensor['sensor_id']}: {e}")
        time.sleep(interval)


def main():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "iot-simulator",
        "acks": "1",
        "linger.ms": 5,
        "batch.size": 65536,
    }
    producer = Producer(conf)
    sensors = SENSOR_LOCATIONS[:SENSOR_COUNT]
    logger.info(f"Starting {len(sensors)} sensor threads, interval={EMISSION_INTERVAL_SECONDS}s")

    threads = []
    for sensor in sensors:
        t = threading.Thread(
            target=sensor_worker,
            args=(sensor, producer, EMISSION_INTERVAL_SECONDS),
            daemon=True,
        )
        threads.append(t)
        t.start()
        time.sleep(0.05)  # stagger starts

    try:
        while True:
            producer.flush(timeout=1)
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Shutting down simulator")
        producer.flush()


if __name__ == "__main__":
    main()
