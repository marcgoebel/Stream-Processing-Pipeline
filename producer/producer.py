"""
Kafka Producer — Simuliert IoT-Sensor-Datenstrom

Liest Umwelt-Sensordaten aus einer CSV-Datei und veröffentlicht jeden Datensatz
als JSON-Nachricht an ein Kafka-Topic. Simuliert damit einen Echtzeit-Datenstrom
von städtischen Umweltüberwachungssensoren.

Architektur:
    CSV-Datei → Producer → Kafka Topic ("sensor-data")
"""

import csv
import json
import logging
import os
import sys
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Konfiguration über Umgebungsvariablen ────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
DATA_FILE = os.getenv("DATA_FILE", "/app/data/iot_telemetry_data.csv")
STREAM_DELAY = float(os.getenv("STREAM_DELAY", "0.01"))  # Sekunden zwischen Nachrichten

# ── Logging-Konfiguration ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_producer(max_retries: int = 30, retry_interval: int = 5) -> KafkaProducer:
    """
    Erstellt einen Kafka-Producer mit Wiederholungslogik.

    Kafka ist beim Container-Start möglicherweise noch nicht verfügbar,
    daher wird die Verbindung mehrfach versucht.
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Warte auf Bestätigung aller Replikate
                retries=3,
            )
            logger.info(f"Verbunden mit Kafka unter {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka nicht bereit (Versuch {attempt}/{max_retries}), "
                f"neuer Versuch in {retry_interval}s..."
            )
            time.sleep(retry_interval)

    logger.error("Verbindung zu Kafka nach maximalen Versuchen fehlgeschlagen.")
    sys.exit(1)


def parse_csv_row(row: dict) -> dict:
    """
    Wandelt eine CSV-Zeile in einen korrekt typisierten Sensormesswert um.

    Konvertiert String-Werte aus der CSV in passende Python-Typen
    (float, bool) für saubere JSON-Serialisierung an Kafka.
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
    Liest Sensordaten aus CSV und veröffentlicht jede Zeile an Kafka.

    Verwendet die device_id als Kafka-Nachrichtenschlüssel, damit alle
    Messwerte eines Sensors in derselben Partition landen und die
    sensorspezifische Reihenfolge erhalten bleibt.
    """
    logger.info(f"Lese Daten aus {DATA_FILE}")
    logger.info(f"Veröffentliche an Topic '{KAFKA_TOPIC}' mit {STREAM_DELAY}s Verzögerung")

    sent_count = 0
    error_count = 0

    with open(DATA_FILE, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                message = parse_csv_row(row)

                # device_id als Schlüssel → sensorspezifische Reihenfolge
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=message["device_id"],
                    value=message,
                )
                sent_count += 1

                if sent_count % 5000 == 0:
                    producer.flush()
                    logger.info(f"{sent_count:,} Nachrichten veröffentlicht...")

                # Echtzeit-Verzögerung zwischen Sensormesswerten simulieren
                if STREAM_DELAY > 0:
                    time.sleep(STREAM_DELAY)

            except Exception as e:
                error_count += 1
                logger.error(f"Fehler beim Senden der Nachricht: {e}")
                if error_count > 100:
                    logger.error("Zu viele Fehler, Producer wird gestoppt.")
                    break

    # Letzter Flush um sicherzustellen, dass alle gepufferten Nachrichten gesendet werden
    producer.flush()
    logger.info(
        f"Streaming abgeschlossen: {sent_count:,} Nachrichten gesendet, "
        f"{error_count} Fehler"
    )


def main():
    logger.info("=" * 60)
    logger.info("IoT-Sensor-Daten-Producer wird gestartet...")
    logger.info(f"  Kafka:   {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic:   {KAFKA_TOPIC}")
    logger.info(f"  Quelle:  {DATA_FILE}")
    logger.info(f"  Verzögerung: {STREAM_DELAY}s pro Nachricht")
    logger.info("=" * 60)

    producer = create_producer()
    stream_data(producer)
    producer.close()

    logger.info("Producer ordnungsgemäß heruntergefahren.")


if __name__ == "__main__":
    main()
