"""
Kafka Consumer — Schreibt Sensordaten in MongoDB

Konsumiert Umwelt-Sensormesswerte aus dem Kafka-Topic und speichert
sie in MongoDB. Implementiert Batch-Schreibvorgänge für bessere
Performance und fügt Metadaten (Aufnahmezeitstempel, Alarm-Flags)
während der Verarbeitung hinzu.

Architektur:
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

# ── Konfiguration über Umgebungsvariablen ────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sensor-consumer-group")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot_sensor_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sensor_readings")

# Batch-Größe für MongoDB-Masseninserts
BATCH_SIZE = 500

# Alarm-Grenzwerte für Umweltmesswerte
ALERT_THRESHOLDS = {
    "temperature": {"min": -10, "max": 40, "unit": "°C"},
    "humidity": {"min": 20, "max": 90, "unit": "%"},
    "co": {"min": 0, "max": 0.02, "unit": "ppm"},
    "smoke": {"min": 0, "max": 0.05, "unit": "ppm"},
    "noise_db": {"min": 0, "max": 85, "unit": "dB"},
}

# ── Logging-Konfiguration ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def connect_to_mongodb(max_retries: int = 20, retry_interval: int = 5) -> MongoClient:
    """
    Verbindet sich mit MongoDB mit Wiederholungslogik.

    Gibt einen pymongo MongoClient zurück, nachdem die Verbindung verifiziert wurde.
    """
    for attempt in range(1, max_retries + 1):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            logger.info(f"Verbunden mit MongoDB unter {MONGO_URI}")
            return client
        except ConnectionFailure:
            logger.warning(
                f"MongoDB nicht bereit (Versuch {attempt}/{max_retries}), "
                f"neuer Versuch in {retry_interval}s..."
            )
            time.sleep(retry_interval)

    logger.error("Verbindung zu MongoDB nach maximalen Versuchen fehlgeschlagen.")
    sys.exit(1)


def setup_collection(db) -> None:
    """
    Richtet die MongoDB-Collection mit Indizes für effiziente Abfragen ein.

    Erstellt Indizes auf häufig abgefragten Feldern:
    - device_id + ts: für Zeitreihenabfragen pro Sensor
    - location: für geografische Abfragen
    - alerts: für schnelle Alarm-Abfragen
    """
    collection = db[MONGO_COLLECTION]

    collection.create_index([("device_id", ASCENDING), ("ts", ASCENDING)])
    collection.create_index([("location", ASCENDING)])
    collection.create_index([("ts", ASCENDING)])
    collection.create_index([("alerts", ASCENDING)], sparse=True)

    logger.info(f"Indizes für Collection '{MONGO_COLLECTION}' erstellt.")


def create_consumer(max_retries: int = 30, retry_interval: int = 5) -> KafkaConsumer:
    """
    Erstellt einen Kafka-Consumer mit Wiederholungslogik.
    """
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",  # Lese von Anfang an
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=60000,  # Stoppe nach 60s ohne Nachrichten
            )
            logger.info(f"Abonniert auf Kafka-Topic '{KAFKA_TOPIC}'")
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka nicht bereit (Versuch {attempt}/{max_retries}), "
                f"neuer Versuch in {retry_interval}s..."
            )
            time.sleep(retry_interval)

    logger.error("Verbindung zu Kafka nach maximalen Versuchen fehlgeschlagen.")
    sys.exit(1)


def check_alerts(reading: dict) -> list:
    """
    Prüft ob Sensormesswerte empfohlene Grenzwerte überschreiten.

    Gibt eine Liste von Alarm-Strings für Werte außerhalb sicherer Bereiche zurück.
    Dies simuliert das Bürger-Warnsystem der Stadtverwaltung.
    """
    alerts = []
    for metric, limits in ALERT_THRESHOLDS.items():
        value = reading.get(metric)
        if value is not None:
            if value > limits["max"]:
                alerts.append(
                    f"{metric}_hoch: {value}{limits['unit']} überschreitet {limits['max']}{limits['unit']}"
                )
            elif value < limits["min"]:
                alerts.append(
                    f"{metric}_niedrig: {value}{limits['unit']} unter {limits['min']}{limits['unit']}"
                )
    return alerts


def enrich_reading(reading: dict) -> dict:
    """
    Reichert einen Sensormesswert mit Verarbeitungsmetadaten an.

    Fügt hinzu:
    - ingested_at: Zeitstempel der Datenverarbeitung
    - alerts: Liste der Grenzwertüberschreitungen (leer wenn alle Werte normal)
    """
    reading["ingested_at"] = datetime.now(timezone.utc).isoformat()
    reading["alerts"] = check_alerts(reading)
    return reading


def consume_and_store(consumer: KafkaConsumer, collection) -> None:
    """
    Konsumiert Nachrichten aus Kafka und fügt sie batchweise in MongoDB ein.

    Verwendet Batch-Inserts (BATCH_SIZE pro Durchgang) für bessere
    MongoDB-Schreibperformance im Vergleich zu Einzelinserts.
    """
    batch = []
    total_consumed = 0
    total_alerts = 0

    logger.info(f"Konsumiere Nachrichten (Batch-Größe: {BATCH_SIZE})...")

    for message in consumer:
        reading = enrich_reading(message.value)
        batch.append(reading)

        if reading["alerts"]:
            total_alerts += len(reading["alerts"])

        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch)
            total_consumed += len(batch)
            logger.info(
                f"{total_consumed:,} Dokumente eingefügt "
                f"({total_alerts} Alarme erkannt)"
            )
            batch = []

    # Verbleibende Nachrichten im letzten Batch einfügen
    if batch:
        collection.insert_many(batch)
        total_consumed += len(batch)

    logger.info("=" * 60)
    logger.info(f"Konsumierung abgeschlossen!")
    logger.info(f"  Dokumente gesamt:  {total_consumed:,}")
    logger.info(f"  Alarme gesamt:     {total_alerts}")
    logger.info("=" * 60)


def main():
    logger.info("=" * 60)
    logger.info("IoT-Sensor-Daten-Consumer wird gestartet...")
    logger.info(f"  Kafka:      {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic:      {KAFKA_TOPIC}")
    logger.info(f"  Gruppe:     {KAFKA_GROUP_ID}")
    logger.info(f"  MongoDB:    {MONGO_URI}/{MONGO_DB}")
    logger.info(f"  Collection: {MONGO_COLLECTION}")
    logger.info("=" * 60)

    # Mit MongoDB verbinden und Indizes erstellen
    mongo_client = connect_to_mongodb()
    db = mongo_client[MONGO_DB]
    setup_collection(db)
    collection = db[MONGO_COLLECTION]

    # Mit Kafka verbinden und Konsumierung starten
    consumer = create_consumer()
    consume_and_store(consumer, collection)

    consumer.close()
    mongo_client.close()
    logger.info("Consumer ordnungsgemäß heruntergefahren.")


if __name__ == "__main__":
    main()
