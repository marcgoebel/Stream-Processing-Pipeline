"""
Kafka Consumer — Writes Sensor Data to MongoDB

Consumes environmental sensor readings from the Kafka topic and stores
them in MongoDB. Implements batch writing for performance and adds
metadata (ingestion timestamp, alert flags) during processing.

Architecture:
    Kafka Topic ("sensor-data") → Consumer → MongoDB
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure

# ── Configuration via environment variables ──────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sensor-consumer-group")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot_sensor_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sensor_readings")

# Batch size for MongoDB bulk inserts
BATCH_SIZE = 500

# Alert thresholds for environmental metrics
ALERT_THRESHOLDS = {
    "temperature": {"min": -10, "max": 40, "unit": "°C"},
    "humidity": {"min": 20, "max": 90, "unit": "%"},
    "co": {"min": 0, "max": 0.02, "unit": "ppm"},
    "smoke": {"min": 0, "max": 0.05, "unit": "ppm"},
    "noise_db": {"min": 0, "max": 85, "unit": "dB"},
}

# ── Logging setup ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def connect_to_mongodb(max_retries: int = 20, retry_interval: int = 5) -> MongoClient:
    """
    Connect to MongoDB with retry logic.

    Returns a pymongo MongoClient after verifying the connection works.
    """
    for attempt in range(1, max_retries + 1):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            logger.info(f"Connected to MongoDB at {MONGO_URI}")
            return client
        except ConnectionFailure:
            logger.warning(
                f"MongoDB not ready (attempt {attempt}/{max_retries}), "
                f"retrying in {retry_interval}s..."
            )
            time.sleep(retry_interval)

    logger.error("Could not connect to MongoDB after maximum retries.")
    sys.exit(1)


def setup_collection(db) -> None:
    """
    Set up MongoDB collection with indexes for efficient querying.

    Creates indexes on commonly queried fields:
    - device_id + ts: for time-series queries per sensor
    - location: for geographic queries
    - alerts: for quick alert lookups
    """
    collection = db[MONGO_COLLECTION]

    collection.create_index([("device_id", ASCENDING), ("ts", ASCENDING)])
    collection.create_index([("location", ASCENDING)])
    collection.create_index([("ts", ASCENDING)])
    collection.create_index([("alerts", ASCENDING)], sparse=True)

    logger.info(f"Collection '{MONGO_COLLECTION}' indexes created.")


def create_consumer(max_retries: int = 30, retry_interval: int = 5) -> KafkaConsumer:
    """
    Create a Kafka consumer with retry logic.
    """
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",  # read from beginning
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=60000,  # stop after 60s of no messages
            )
            logger.info(f"Subscribed to Kafka topic '{KAFKA_TOPIC}'")
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}), "
                f"retrying in {retry_interval}s..."
            )
            time.sleep(retry_interval)

    logger.error("Could not connect to Kafka after maximum retries.")
    sys.exit(1)


def check_alerts(reading: dict) -> list:
    """
    Check if any sensor values exceed recommended thresholds.

    Returns a list of alert strings for values outside safe ranges.
    This simulates the municipality's citizen warning system.
    """
    alerts = []
    for metric, limits in ALERT_THRESHOLDS.items():
        value = reading.get(metric)
        if value is not None:
            if value > limits["max"]:
                alerts.append(
                    f"{metric}_high: {value}{limits['unit']} exceeds {limits['max']}{limits['unit']}"
                )
            elif value < limits["min"]:
                alerts.append(
                    f"{metric}_low: {value}{limits['unit']} below {limits['min']}{limits['unit']}"
                )
    return alerts


def enrich_reading(reading: dict) -> dict:
    """
    Enrich a sensor reading with processing metadata.

    Adds:
    - ingested_at: timestamp when the data was processed
    - alerts: list of threshold violations (empty if all values normal)
    """
    reading["ingested_at"] = datetime.now(timezone.utc).isoformat()
    reading["alerts"] = check_alerts(reading)
    return reading


def consume_and_store(consumer: KafkaConsumer, collection) -> None:
    """
    Consume messages from Kafka and batch-insert into MongoDB.

    Uses batch inserts (BATCH_SIZE at a time) for better MongoDB
    write performance compared to individual inserts.
    """
    batch = []
    total_consumed = 0
    total_alerts = 0

    logger.info(f"Consuming messages (batch size: {BATCH_SIZE})...")

    for message in consumer:
        reading = enrich_reading(message.value)
        batch.append(reading)

        if reading["alerts"]:
            total_alerts += len(reading["alerts"])

        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch)
            total_consumed += len(batch)
            logger.info(
                f"Inserted {total_consumed:,} documents "
                f"({total_alerts} alerts detected)"
            )
            batch = []

    # Insert remaining messages in the last batch
    if batch:
        collection.insert_many(batch)
        total_consumed += len(batch)

    logger.info("=" * 60)
    logger.info(f"Consumption complete!")
    logger.info(f"  Total documents:  {total_consumed:,}")
    logger.info(f"  Total alerts:     {total_alerts}")
    logger.info("=" * 60)


def main():
    logger.info("=" * 60)
    logger.info("IoT Sensor Data Consumer starting...")
    logger.info(f"  Kafka:      {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic:      {KAFKA_TOPIC}")
    logger.info(f"  Group:      {KAFKA_GROUP_ID}")
    logger.info(f"  MongoDB:    {MONGO_URI}/{MONGO_DB}")
    logger.info(f"  Collection: {MONGO_COLLECTION}")
    logger.info("=" * 60)

    # Connect to MongoDB and set up indexes
    mongo_client = connect_to_mongodb()
    db = mongo_client[MONGO_DB]
    setup_collection(db)
    collection = db[MONGO_COLLECTION]

    # Connect to Kafka and start consuming
    consumer = create_consumer()
    consume_and_store(consumer, collection)

    consumer.close()
    mongo_client.close()
    logger.info("Consumer shut down gracefully.")


if __name__ == "__main__":
    main()
