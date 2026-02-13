"""
Kafka Producer — Simulates IoT Sensor Data Stream

Reads environmental sensor data from a CSV file and publishes each record
to a Kafka topic as a JSON message, simulating a near real-time sensor
data stream from city-wide environmental monitoring sensors.

Architecture:
    CSV File → Producer → Kafka Topic ("sensor-data")
"""

import csv
import json
import logging
import os
import sys
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration via environment variables ──────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
DATA_FILE = os.getenv("DATA_FILE", "/app/data/iot_telemetry_data.csv")
STREAM_DELAY = float(os.getenv("STREAM_DELAY", "0.01"))  # seconds between messages

# ── Logging setup ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_producer(max_retries: int = 30, retry_interval: int = 5) -> KafkaProducer:
    """
    Create a Kafka producer with retry logic.

    Kafka may not be immediately available when the container starts,
    so we retry the connection with exponential backoff.
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # wait for all replicas to acknowledge
                retries=3,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}), "
                f"retrying in {retry_interval}s..."
            )
            time.sleep(retry_interval)

    logger.error("Could not connect to Kafka after maximum retries.")
    sys.exit(1)


def parse_csv_row(row: dict) -> dict:
    """
    Parse a CSV row into a properly typed sensor reading.

    Converts string values from CSV to appropriate Python types
    (float, bool) for clean JSON serialization to Kafka.
    """
    return {
        "ts": row["ts"],
        "device_id": row["device_id"],
        "location": row["location"],
        "lat": float(row["lat"]),
        "lon": float(row["lon"]),
        "temperature": float(row["temperature"]),
        "humidity": float(row["humidity"]),
        "co": float(row["co"]),
        "smoke": float(row["smoke"]),
        "lpg": float(row["lpg"]),
        "noise_db": float(row["noise_db"]),
        "light": row["light"] == "True",
        "motion": row["motion"] == "True",
    }


def stream_data(producer: KafkaProducer) -> None:
    """
    Read sensor data from CSV and publish each row to Kafka.

    Uses the device_id as the Kafka message key to ensure all readings
    from the same sensor end up in the same partition, preserving
    per-device ordering.
    """
    logger.info(f"Reading data from {DATA_FILE}")
    logger.info(f"Publishing to topic '{KAFKA_TOPIC}' with {STREAM_DELAY}s delay")

    sent_count = 0
    error_count = 0

    with open(DATA_FILE, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                message = parse_csv_row(row)

                # Use device_id as key → ensures per-device ordering
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=message["device_id"],
                    value=message,
                )
                sent_count += 1

                if sent_count % 5000 == 0:
                    producer.flush()
                    logger.info(f"Published {sent_count:,} messages...")

                # Simulate real-time delay between sensor readings
                if STREAM_DELAY > 0:
                    time.sleep(STREAM_DELAY)

            except Exception as e:
                error_count += 1
                logger.error(f"Error sending message: {e}")
                if error_count > 100:
                    logger.error("Too many errors, stopping producer.")
                    break

    # Final flush to ensure all buffered messages are sent
    producer.flush()
    logger.info(
        f"Streaming complete: {sent_count:,} messages sent, "
        f"{error_count} errors"
    )


def main():
    logger.info("=" * 60)
    logger.info("IoT Sensor Data Producer starting...")
    logger.info(f"  Kafka:   {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic:   {KAFKA_TOPIC}")
    logger.info(f"  Source:  {DATA_FILE}")
    logger.info(f"  Delay:   {STREAM_DELAY}s per message")
    logger.info("=" * 60)

    producer = create_producer()
    stream_data(producer)
    producer.close()

    logger.info("Producer shut down gracefully.")


if __name__ == "__main__":
    main()
