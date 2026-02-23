# IoT Sensor Streaming Pipeline

Eine containerisierte Stream-Processing-Pipeline für Umwelt-IoT-Sensordaten, gebaut mit **Apache Kafka**, **MongoDB** und **Docker**.

## Szenario

Eine Stadtverwaltung hat Umweltsensoren in der Stadt installiert, die Temperatur, Luftfeuchtigkeit, Kohlenmonoxid, Rauch, LPG, Lärmpegel, Licht und Bewegung messen. Dieses System nimmt die Sensordaten als Echtzeit-Stream auf, verarbeitet sie und speichert sie in einer Datenbank für Frontend-Anwendungen und Bürger-Warnsysteme.

## Architektur

```
┌──────────────────┐     ┌────────────────────────────┐     ┌──────────────────┐
│  Python Producer  │────▶│  Apache Kafka               │────▶│  Python Consumer  │
│  (simuliert IoT-  │     │  Topic: "sensor-data"       │     │  (reichert Daten  │
│   Sensor-Stream)  │     │  Broker: Confluent 7.6       │     │   an, schreibt    │
└──────────────────┘     └────────────────────────────┘     │   in Datenbank)   │
        │                         │                          └────────┬─────────┘
   Liest CSV                 Zookeeper                                │
   (100K+ Datensätze)        (Koordination)                    ┌──────▼───────┐
                                                               │   MongoDB 7   │
                                                               │   (Dokument-  │
                                                               │    speicher)  │
                                                               └──────────────┘
```

## Technologie-Stack

| Komponente     | Technologie                   | Zweck                                  |
|----------------|-------------------------------|----------------------------------------|
| Streaming      | Apache Kafka (Confluent 7.6)  | Nachrichtenbroker für Sensordaten      |
| Koordination   | Zookeeper                     | Kafka-Cluster-Koordination             |
| Datenbank      | MongoDB 7.0                   | Flexibler Dokumentenspeicher           |
| Producer       | Python 3.11 + kafka-python    | Simuliert Echtzeit-Sensor-Stream       |
| Consumer       | Python 3.11 + pymongo         | Verarbeitet und speichert Sensordaten  |
| Container      | Docker + Docker Compose       | Portable, reproduzierbare Bereitstellung|

## Voraussetzungen

- [Docker](https://www.docker.com/get-started) (v20.10+)
- [Docker Compose](https://docs.docker.com/compose/) (v2.0+)

Keine weiteren Abhängigkeiten nötig — alles läuft in Containern.

## Schnellstart

```bash
# 1. Repository klonen
git clone https://github.com/marcgoebel/Stream-Processing-Pipeline.git
cd Stream-Processing-Pipeline

# 2. Gesamte Pipeline starten
docker-compose up --build

# 3. (Optional) Im Hintergrund starten
docker-compose up --build -d

# 4. Logs prüfen
docker-compose logs -f producer
docker-compose logs -f consumer

# 5. Pipeline stoppen
docker-compose down

# 6. Pipeline stoppen und alle Daten löschen
docker-compose down -v
```

## Datensatz

Der Beispieldatensatz enthält **100.800 Sensormesswerte** von 5 simulierten Sensoren über 7 Tage, generiert durch `scripts/generate_sample_data.py`.

### Sensor-Standorte

| Geräte-ID   | Standort            | Beschreibung                          |
|-------------|---------------------|---------------------------------------|
| sensor-001  | city-center         | Innenstadt, mittlere Belastung        |
| sensor-002  | industrial-zone     | Industriegebiet, höhere Emissionen    |
| sensor-003  | residential-north   | Wohngebiet, geringe Belastung         |
| sensor-004  | park-area           | Grünfläche, sauberste Luft            |
| sensor-005  | highway-bridge      | Verkehrszone, hoher Lärm + CO         |

### Datenschema

| Feld         | Typ     | Beschreibung                          |
|--------------|---------|---------------------------------------|
| ts           | String  | ISO 8601 Zeitstempel                  |
| device_id    | String  | Eindeutige Sensor-Kennung             |
| location     | String  | Name des Einsatzortes                 |
| lat          | Float   | Breitengrad                           |
| lon          | Float   | Längengrad                            |
| temperature  | Float   | Temperatur in °C                      |
| humidity     | Float   | Relative Luftfeuchtigkeit in %        |
| co           | Float   | Kohlenmonoxid in ppm                  |
| smoke        | Float   | Rauchpegel in ppm                     |
| lpg          | Float   | Flüssiggas in ppm                     |
| noise_db     | Float   | Lärmpegel in dB                       |
| light        | Bool    | Tageslicht erkannt                    |
| motion       | Bool    | Bewegung erkannt                      |

## Datenverarbeitung

Der Consumer reichert jede Messung an mit:

- **`ingested_at`**: UTC-Zeitstempel der Verarbeitung
- **`alerts`**: Liste der Grenzwertüberschreitungen (z.B. CO über sicherem Niveau)

### Alarm-Grenzwerte

| Messgröße    | Sicherer Bereich | Alarm-Bedingung              |
|--------------|------------------|------------------------------|
| Temperatur   | -10°C bis 40°C   | Außerhalb → Alarm            |
| Feuchtigkeit | 20% bis 90%      | Außerhalb → Alarm            |
| CO           | 0 bis 0,02 ppm   | Überschreitung → Alarm       |
| Rauch        | 0 bis 0,05 ppm   | Überschreitung → Alarm       |
| Lärm         | 0 bis 85 dB      | Überschreitung → Alarm       |

## Projektstruktur

```
Stream-Processing-Pipeline/
├── docker-compose.yml          # Orchestriert alle 5 Dienste
├── producer/
│   ├── Dockerfile              # Producer-Container-Definition
│   ├── producer.py             # Kafka-Producer-Anwendung
│   └── requirements.txt        # Python-Abhängigkeiten
├── consumer/
│   ├── Dockerfile              # Consumer-Container-Definition
│   ├── consumer.py             # Kafka-Consumer-Anwendung
│   └── requirements.txt        # Python-Abhängigkeiten
├── data/
│   └── iot_telemetry_data.csv  # Beispiel-Sensordaten (100K+ Datensätze)
├── scripts/
│   └── generate_sample_data.py # Datengenerierungsskript
└── README.md                   # Diese Datei
```

## Daten abfragen

Nachdem die Pipeline gelaufen ist, kann MongoDB abgefragt werden:

```bash
# Mit MongoDB im Container verbinden
docker exec -it mongodb mongosh iot_sensor_db

# Alle Dokumente zählen
db.sensor_readings.countDocuments()

# Messwerte mit Alarmen finden
db.sensor_readings.find({ "alerts": { $ne: [] } }).limit(5)

# Durchschnittstemperatur pro Standort
db.sensor_readings.aggregate([
  { $group: { _id: "$location", avg_temp: { $avg: "$temperature" } } }
])

# Messwerte eines bestimmten Sensors in einem Zeitraum
db.sensor_readings.find({
  device_id: "sensor-002",
  ts: { $gte: "2024-07-03", $lte: "2024-07-04" }
}).count()
```

## Entwurfsentscheidungen

### Warum Kafka?
- Industriestandard für Stream-Verarbeitung mit starken Haltbarkeitsgarantien
- Unterstützt Partitionierung nach Geräte-ID für sensorspezifische Reihenfolge
- Horizontal skalierbar — mehr Broker erhöhen den Durchsatz linear
- Entkoppelt Producer von Consumer und ermöglicht unabhängige Skalierung

### Warum MongoDB?
- **Schema-Flexibilität**: Neue Sensortypen (CO₂, Feinstaub) ohne Migration hinzufügbar
- **Dokumentenmodell**: Sensormesswerte mit verschachtelten Alarmen passen natürlich ins Dokumentformat
- **Horizontale Skalierung**: Eingebautes Sharding für verteilte Bereitstellungen
- **Indizierung**: Zusammengesetzte Indizes auf (device_id, ts) ermöglichen effiziente Zeitreihenabfragen

### Warum Docker Compose?
- Ein einziger Befehl (`docker-compose up`) startet die gesamte Pipeline
- Keine Host-Abhängigkeiten außer Docker selbst
- Portabel über Betriebssysteme hinweg (Linux, macOS, Windows)
- Health Checks stellen die korrekte Startreihenfolge der Dienste sicher

## Lizenz

Dieses Projekt dient Bildungszwecken im Rahmen des IU Data Engineering Kurses (DLBDSEDE02).
