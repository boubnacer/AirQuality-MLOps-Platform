import json
from loguru import logger
from confluent_kafka import Producer

THRESHOLDS = {
    "pm2_5": {"warning": 25.0, "critical": 75.0},
    "pm10":  {"warning": 50.0, "critical": 150.0},
    "no2":   {"warning": 100.0, "critical": 200.0},
    "o3":    {"warning": 120.0, "critical": 180.0},
    "co":    {"warning": 4.0,   "critical": 10.0},
    "so2":   {"warning": 125.0, "critical": 350.0},
}


def check_anomalies(payload: dict, producer: Producer) -> list:
    """Check rule-based thresholds, publish anomalies to Kafka, return list of anomaly dicts."""
    anomalies = []
    measurements = payload.get("measurements", {})
    sensor_id = payload.get("sensor_id")
    timestamp = payload.get("timestamp")

    for pollutant, thresholds in THRESHOLDS.items():
        value = measurements.get(pollutant)
        if value is None:
            continue

        severity = None
        threshold_used = None
        if value >= thresholds["critical"]:
            severity = "critical"
            threshold_used = thresholds["critical"]
        elif value >= thresholds["warning"]:
            severity = "warning"
            threshold_used = thresholds["warning"]

        if severity:
            anomaly = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "anomaly_type": "rule_based",
                "severity": severity,
                "pollutant": pollutant,
                "value": value,
                "threshold": threshold_used,
                "location": payload.get("location", {}),
            }
            anomalies.append(anomaly)
            try:
                producer.produce(
                    "anomalies",
                    key=sensor_id,
                    value=json.dumps(anomaly),
                )
                producer.poll(0)
            except Exception as e:
                logger.error(f"Failed to publish anomaly: {e}")

    return anomalies
