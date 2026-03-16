#!/bin/bash
set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "Waiting for Kafka to be ready..."
sleep 10

docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists --topic raw_data \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists --topic processed_data \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000

docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists --topic anomalies \
  --partitions 1 --replication-factor 1 \
  --config retention.ms=2592000000

echo "Kafka topics created successfully"
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --list