"""
Generiert alle drei Portfolio-PDF-Dokumente gemäß IU-Formalvorgaben.

Formatvorgaben:
- Papier: DIN A4
- Ränder: 2cm allseitig
- Schrift: Arial 11pt (Fließtext), 12pt (Überschriften)
- Zeilenabstand: 1,5
- Ausrichtung: Blocksatz
"""

from fpdf import FPDF
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs")


class PortfolioPDF(FPDF):
    """Basis-PDF-Klasse mit IU-Formatierungsvorgaben."""

    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=20)
        self.set_margins(20, 20, 20)

    def heading(self, text, level=1):
        size = 12
        self.set_font("Arial", "B", size)
        self.cell(0, 8, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text):
        self.set_font("Arial", "", 11)
        self.multi_cell(0, 6.5, text, align="J")  # 6,5mm ~ 1,5-facher Zeilenabstand bei 11pt
        self.ln(2)

    def small_heading(self, text):
        self.set_font("Arial", "B", 11)
        self.cell(0, 6.5, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)


def generate_phase1():
    """Phase 1: Konzeption — 1/2 Seite Konzepttext."""
    pdf = PortfolioPDF()
    pdf.add_page()

    pdf.heading("Konzeption: Stream-Processing-Pipeline für kommunale IoT-Sensordaten")

    pdf.body_text(
        "Ziel dieses Projekts ist der Entwurf und die Implementierung einer containerisierten "
        "Stream-Processing-Pipeline, die Umwelt-Sensordaten einer Stadtverwaltung aufnimmt, "
        "verarbeitet und speichert. Sensoren an fünf Stadtstandorten messen Temperatur, "
        "Luftfeuchtigkeit, Kohlenmonoxid, Rauch, LPG, Lärmpegel, Licht und Bewegung. Das System "
        "muss kontinuierliche Datenströme zuverlässig verarbeiten und die Messwerte so speichern, "
        "dass zukünftige Frontend-Anwendungen für Stadtplaner und ein Bürger-Warnsystem bei "
        "Grenzwertüberschreitungen unterstützt werden."
    )

    pdf.body_text(
        "Die Pipeline verwendet Apache Kafka als zentralen Nachrichtenbroker. Kafka wurde gegenüber "
        "Spark Streaming gewählt, da es ein einfacheres Bereitstellungsmodell für das hier benötigte "
        "Producer-Consumer-Muster bietet, starke Haltbarkeitsgarantien durch Nachrichtenpersistierung "
        "gewährleistet und horizontale Skalierung durch Hinzufügen von Brokern ermöglicht, ohne die "
        "Architektur umzugestalten. Kafkas Partitionierung nach Geräte-ID stellt die sensorspezifische "
        "Nachrichtenreihenfolge sicher, was für die Integrität der Zeitreihen entscheidend ist."
    )

    pdf.body_text(
        "Als Datenbank wurde MongoDB gewählt. Sein Dokumentenmodell eignet sich natürlich für die "
        "heterogenen Sensordaten: Aktuelle Messwerte umfassen numerische und boolesche Felder, und "
        "zukünftige Sensoren (CO2, Feinstaub) können ohne Schema-Migrationen hinzugefügt werden. "
        "MongoDB unterstützt horizontale Skalierung durch Sharding und bietet zusammengesetzte Indizes "
        "auf Geräte-ID und Zeitstempel für effiziente Zeitreihenabfragen. Alternativen wie PostgreSQL "
        "wurden erwogen, aber aufgrund der starren Schema-Anforderungen abgelehnt, die das Hinzufügen "
        "neuer Sensortypen erschweren würden."
    )

    pdf.body_text(
        "Das gesamte System ist mit Docker Compose containerisiert und orchestriert fünf Dienste: "
        "Zookeeper (Kafka-Koordination), Kafka (Nachrichtenbroker), MongoDB (Speicher), einen Python-"
        "Producer (simuliert Sensor-Stream aus CSV-Daten) und einen Python-Consumer (liest aus Kafka, "
        "reichert Daten mit Alarm-Flags an und schreibt in MongoDB). Health Checks stellen die korrekte "
        "Startreihenfolge sicher. Der Implementierungsplan umfasst drei Schritte: erstens Aufbau der "
        "Container-Infrastruktur, zweitens Implementierung der Producer- und Consumer-Anwendungen, "
        "drittens Testen und Optimieren der Ende-zu-Ende-Pipeline."
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "Goebel-Marc_IU14074686_ProjektDataEngineering_P1_S.pdf")
    pdf.output(path)
    print(f"Phase 1 gespeichert: {path}")
    return path


def generate_phase2():
    """Phase 2: Entwicklung — 1/2 Seite Erklärungstext."""
    pdf = PortfolioPDF()
    pdf.add_page()

    pdf.heading("Entwicklung: Implementierung der Stream-Processing-Pipeline")

    pdf.body_text(
        "Die Implementierung folgt dem Konzept aus Phase 1. Das gesamte System ist in einer einzigen "
        "docker-compose.yml-Datei definiert, die fünf Dienste orchestriert. Zookeeper und Kafka "
        "verwenden offizielle Confluent-Images (v7.6.0), MongoDB das offizielle mongo:7.0-Image, und "
        "Producer sowie Consumer sind eigene Python-3.11-Anwendungen, die aus schlanken Dockerfiles "
        "gebaut werden. Health Checks auf Zookeeper, Kafka und MongoDB stellen sicher, dass abhängige "
        "Dienste erst starten, wenn ihre Abhängigkeiten vollständig betriebsbereit sind."
    )

    pdf.body_text(
        "Ein Python-Skript generiert 100.800 realistische Sensormesswerte an fünf Stadtstandorten "
        "über sieben Tage, mit Tageszeiteffekten auf Temperatur und Lärm. Der Producer liest diese "
        "CSV-Datei und veröffentlicht jeden Datensatz als JSON-Nachricht an das Kafka-Topic "
        "'sensor-data', wobei die Geräte-ID als Partitionsschlüssel dient. Der Consumer abonniert "
        "dieses Topic, deserialisiert die JSON-Nachrichten, reichert jeden Messwert mit einem "
        "Aufnahmezeitstempel und Alarm-Flags bei Grenzwertüberschreitungen an (z.B. CO über "
        "0,02 ppm, Lärm über 85 dB) und fügt jeweils 500 Dokumente als Batch in MongoDB ein."
    )

    pdf.body_text(
        "MongoDB-Indizes auf (device_id, ts), location und alerts ermöglichen effiziente Abfragen "
        "für die geplanten Frontend-Anwendungen. Das System ist vollständig portabel: Durch Klonen "
        "des GitHub-Repositorys und Ausführen von 'docker-compose up --build' wird die gesamte "
        "Pipeline auf jedem Rechner mit Docker reproduziert, unabhängig vom Betriebssystem."
    )

    pdf.body_text(
        "GitHub Repository: https://github.com/marcgoebel/Stream-Processing-Pipeline"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "Goebel-Marc_IU14074686_ProjektDataEngineering_P2_S.pdf")
    pdf.output(path)
    print(f"Phase 2 gespeichert: {path}")
    return path


def generate_phase3():
    """Phase 3: Finalisierung — 2 Seiten Abstract mit Reflexion."""
    pdf = PortfolioPDF()
    pdf.add_page()

    pdf.heading("Stream-Processing-Pipeline für kommunale IoT-Sensordaten")

    # ── 1. Ziel und Konzept ──
    pdf.small_heading("1. Ziel und Konzept")
    pdf.body_text(
        "Dieses Projekt befasst sich mit der Herausforderung, Umwelt-Sensordaten für eine "
        "Stadtverwaltung zu verarbeiten, die IoT-Sensoren in der Stadt installiert hat. Die Sensoren "
        "messen Temperatur, Luftfeuchtigkeit, Kohlenmonoxid, Rauch, LPG, Lärmpegel, Licht und "
        "Bewegung an fünf verschiedenen Standorten: Innenstadt, Industriegebiet, Wohngebiet, "
        "Grünfläche und Autobahnbrücke. Das Projektziel ist der Aufbau eines zuverlässigen, "
        "skalierbaren und wartbaren Datensystems, das Sensormesswerte als kontinuierlichen Stream "
        "aufnimmt, nahezu in Echtzeit verarbeitet und in einer Datenbank speichert, die für "
        "Frontend-Anwendungen für Stadtplaner und ein Bürger-Warnsystem zugänglich ist."
    )

    # ── 2. Technischer Ansatz ──
    pdf.small_heading("2. Technischer Ansatz")
    pdf.body_text(
        "Die Systemarchitektur folgt einem Producer-Consumer-Muster, aufgebaut um Apache Kafka als "
        "zentralen Nachrichtenbroker. Ein Python-Producer liest Sensordaten aus einem CSV-Datensatz "
        "mit 100.800 Datensätzen und veröffentlicht jeden als JSON-Nachricht an das Kafka-Topic "
        "'sensor-data'. Die Geräte-ID dient als Kafka-Partitionsschlüssel, wodurch sichergestellt "
        "wird, dass alle Messwerte eines Sensors geordnet in dieselbe Partition geliefert werden. "
        "Ein Python-Consumer abonniert dieses Topic, deserialisiert die Nachrichten und schreibt "
        "sie in Batches von 500 Dokumenten in MongoDB."
    )
    pdf.body_text(
        "Während der Konsumierung wird jeder Messwert mit zwei Metadatenfeldern angereichert: einem "
        "Aufnahmezeitstempel, der den Verarbeitungszeitpunkt festhält, und einer Alarmliste, die "
        "Grenzwertüberschreitungen kennzeichnet. Die Alarmgrenzen sind konfigurierbar und "
        "überwachen derzeit Temperatur (über 40 Grad Celsius), Luftfeuchtigkeit (über 90 Prozent "
        "oder unter 20 Prozent), Kohlenmonoxid (über 0,02 ppm), Rauch (über 0,05 ppm) und Lärm "
        "(über 85 dB). Im Testlauf lösten etwa 21 Prozent aller Messwerte mindestens einen Alarm "
        "aus, vorwiegend von den Sensoren im Industriegebiet und an der Autobahnbrücke."
    )

    # ── 3. Technologieauswahl ──
    pdf.small_heading("3. Technologieauswahl und Begründung")
    pdf.body_text(
        "Apache Kafka wurde gegenüber Spark Streaming gewählt, da es ein einfacheres "
        "Bereitstellungsmodell für das Producer-Consumer-Muster bietet, alle Nachrichten persistiert "
        "und horizontal durch Hinzufügen von Brokern skaliert. Spark Streaming wäre bei komplexen "
        "Fensteraggregationen geeigneter, aber für die Aufnahme und Weiterleitung von Sensordaten "
        "ist Kafka die effizientere Wahl."
    )
    pdf.body_text(
        "MongoDB wurde aufgrund seiner Schema-Flexibilität gewählt. Neue Sensortypen (CO2, "
        "Feinstaub) können ohne Migrationen hinzugefügt werden. MongoDB unterstützt Sharding, "
        "zusammengesetzte Indizes für Zeitreihenabfragen und ein starkes Ökosystem. PostgreSQL "
        "wurde aufgrund des starren Schemas abgelehnt, Cassandra aufgrund komplexerer "
        "Betriebsanforderungen für den Prototyp-Maßstab."
    )

    # ── 4. Containerisierung ──
    pdf.small_heading("4. Containerisierung und Reproduzierbarkeit")
    pdf.body_text(
        "Die gesamte Pipeline ist mit Docker Compose containerisiert und definiert fünf Dienste: "
        "Zookeeper, Kafka, MongoDB und zwei Python-Container für Producer und Consumer. Health "
        "Checks stellen die korrekte Startreihenfolge sicher und eliminieren Race Conditions. Das "
        "System kann auf jedem Rechner mit Docker reproduziert werden: Repository klonen und "
        "docker-compose up --build ausführen."
    )

    # ── 5. Ergebnisse ──
    pdf.small_heading("5. Ergebnisse")
    pdf.body_text(
        "Die Pipeline hat alle 100.800 Sensormesswerte erfolgreich verarbeitet. MongoDB-Abfragen "
        "bestätigten die korrekte Datenspeicherung, wobei Aggregationspipelines erwartete Muster "
        "zeigten: Das Industriegebiet verzeichnete die höchste Durchschnittstemperatur (21,1 Grad "
        "Celsius) und die höchsten CO-Werte, während die Grünfläche die niedrigsten aufwies "
        "(17,2 Grad Celsius). Die Alarmerkennung identifizierte 1.580 Grenzwertüberschreitungen "
        "allein in den ersten 6.500 Dokumenten, was die Fähigkeit des Systems demonstriert, "
        "Umweltanomalien für die geplante Bürger-Warnanwendung zu erkennen."
    )

    # ── 6. Persönliche Reflexion ──
    pdf.small_heading("6. Persönliche Reflexion")
    pdf.body_text(
        "Dieses Projekt hat mein Verständnis von Stream-Processing-Architekturen und den praktischen "
        "Herausforderungen containerisierter Bereitstellungen vertieft. Die bedeutendste technische "
        "Herausforderung war die Sicherstellung der korrekten Startreihenfolge voneinander abhängiger "
        "Dienste. Mein ursprünglicher Health Check für Zookeeper basierte auf netcat, das im "
        "Confluent-Docker-Image nicht verfügbar war. Ich löste dies durch den Wechsel zu einem "
        "bash-basierten TCP-Check, was mich lehrte, dass Container-Images oft Werkzeuge vermissen "
        "lassen, die auf Standard-Linux-Systemen als vorhanden angenommen werden."
    )
    pdf.body_text(
        "Eine weitere Herausforderung war die Batch-Größe für MongoDB-Inserts. Kleinere Batches "
        "bieten geringere Latenz, größere besseren Durchsatz. Ich wählte 500 Dokumente als "
        "Kompromiss für den Prototyp-Maßstab. In Produktion müsste dies basierend auf "
        "Nachrichtenraten und Latenzanforderungen angepasst werden."
    )
    pdf.body_text(
        "Meine Problemlösungsstrategie für zukünftige Projekte konzentriert sich auf drei "
        "Prinzipien: erstens mit einem minimalen Prototyp beginnen; zweitens Health Checks und "
        "Abhängigkeitsverwaltung für robuste containerisierte Systeme nutzen; drittens frühzeitig "
        "realistische Beispieldaten generieren, um die Pipeline Ende-zu-Ende zu validieren. "
        "Dieses Projekt hat meine Data-Engineering-Fähigkeiten in Kafka, MongoDB, Docker und "
        "Python gestärkt, die direkt auf reale Dateninfrastruktur-Aufgaben anwendbar sind."
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "Goebel-Marc_IU14074686_ProjektDataEngineering_P3_S.pdf")
    pdf.output(path)
    print(f"Phase 3 gespeichert: {path}")
    return path


if __name__ == "__main__":
    generate_phase1()
    generate_phase2()
    generate_phase3()
    print("\nAlle Portfolio-PDFs erfolgreich generiert!")
