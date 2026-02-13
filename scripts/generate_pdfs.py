"""
Generate all three portfolio PDF documents according to IU formal requirements.

Format specs:
- Paper: DIN A4
- Margins: 2cm all sides
- Font: Arial 11pt (body), 12pt (headings)
- Line spacing: 1.5
- Alignment: Justified
"""

from fpdf import FPDF
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs")


class PortfolioPDF(FPDF):
    """Base PDF class with IU formatting requirements."""

    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=20)
        self.set_margins(20, 20, 20)

    def heading(self, text, level=1):
        size = 14 if level == 1 else 12
        self.set_font("Arial", "B", size)
        self.cell(0, 8, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text):
        self.set_font("Arial", "", 11)
        self.multi_cell(0, 6.5, text, align="J")  # 6.5mm ~= 1.5 line spacing at 11pt
        self.ln(2)

    def small_heading(self, text):
        self.set_font("Arial", "B", 11)
        self.cell(0, 6.5, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)


def generate_phase1():
    """Phase 1: Conception — 1/2 page concept text."""
    pdf = PortfolioPDF()
    pdf.add_page()

    pdf.heading("Conception: Stream Processing Pipeline for Municipal IoT Sensor Data")

    pdf.body_text(
        "The goal of this project is to design and implement a containerized stream processing pipeline "
        "that ingests, processes, and stores environmental sensor data collected by a municipality. "
        "Sensors deployed at five city locations measure temperature, humidity, carbon monoxide, smoke, "
        "LPG, noise levels, light, and motion. The system must reliably handle continuous data streams "
        "and store the readings in a way that supports future front-end applications for city planners "
        "and a citizen alerting system for threshold violations."
    )

    pdf.body_text(
        "The pipeline uses Apache Kafka as the central message broker. Kafka was chosen over Spark Streaming "
        "because it provides a simpler deployment model for the producer-consumer pattern required here, "
        "offers strong durability guarantees through message persistence, and enables horizontal scaling "
        "by adding brokers without redesigning the architecture. Kafka's partitioning by device ID ensures "
        "per-sensor message ordering, which is critical for time-series integrity."
    )

    pdf.body_text(
        "For storage, MongoDB was selected as the database. Its document model naturally accommodates "
        "the heterogeneous sensor data: current readings include numeric and boolean fields, and future "
        "sensors (CO2, fine dust) can be added without schema migrations. MongoDB supports horizontal "
        "scaling through sharding and provides compound indexes on device ID and timestamp for efficient "
        "time-series queries. Alternatives like PostgreSQL were considered but rejected due to the rigid "
        "schema requirements that would complicate the addition of new sensor types."
    )

    pdf.body_text(
        "The entire system is containerized using Docker Compose, orchestrating five services: Zookeeper "
        "(Kafka coordination), Kafka (message broker), MongoDB (storage), a Python producer (simulates "
        "sensor stream from CSV data), and a Python consumer (reads from Kafka, enriches data with alert "
        "flags, and writes to MongoDB). Health checks ensure correct startup order. The implementation "
        "plan proceeds in three steps: first, setting up the container infrastructure; second, implementing "
        "the producer and consumer applications; third, testing and optimizing the end-to-end pipeline."
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "Phase1_Conception.pdf")
    pdf.output(path)
    print(f"Phase 1 saved: {path}")
    return path


def generate_phase2():
    """Phase 2: Development — 1/2 page explanation text."""
    pdf = PortfolioPDF()
    pdf.add_page()

    pdf.heading("Development: Implementation of the Stream Processing Pipeline")

    pdf.body_text(
        "The implementation follows the concept from Phase 1. The entire system is defined in a single "
        "docker-compose.yml file that orchestrates five services. Zookeeper and Kafka use official "
        "Confluent images (v7.6.0), MongoDB uses the official mongo:7.0 image, and the producer and "
        "consumer are custom Python 3.11 applications built from lightweight Dockerfiles. Health checks "
        "on Zookeeper, Kafka, and MongoDB ensure that dependent services only start when their "
        "dependencies are fully operational."
    )

    pdf.body_text(
        "A Python script generates 100,800 realistic sensor readings across five city locations over "
        "seven days, with time-of-day effects on temperature and noise. The producer reads this CSV "
        "file and publishes each record as a JSON message to the Kafka topic 'sensor-data', using the "
        "device ID as the partition key. The consumer subscribes to this topic, deserializes the JSON "
        "messages, enriches each reading with an ingestion timestamp and alert flags for threshold "
        "violations (e.g., CO above 0.02 ppm, noise above 85 dB), and batch-inserts 500 documents "
        "at a time into MongoDB for write performance."
    )

    pdf.body_text(
        "MongoDB indexes on (device_id, ts), location, and alerts enable efficient queries for the "
        "planned front-end applications. The system is fully portable: cloning the GitHub repository "
        "and running 'docker-compose up --build' reproduces the entire pipeline on any machine with "
        "Docker installed, regardless of the operating system."
    )

    pdf.body_text(
        "GitHub Repository: https://github.com/marcgoebel/Stream-Processing-Pipeline"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "Phase2_Development.pdf")
    pdf.output(path)
    print(f"Phase 2 saved: {path}")
    return path


def generate_phase3():
    """Phase 3: Finalization — 2 page abstract with reflection."""
    pdf = PortfolioPDF()
    pdf.add_page()

    pdf.heading("Stream Processing Pipeline for Municipal IoT Sensor Data")

    # ── 1. Objective & Concept ──
    pdf.small_heading("1. Objective and Concept")
    pdf.body_text(
        "This project addresses the challenge of processing environmental sensor data for a municipality "
        "that has deployed IoT sensors across the city. The sensors measure temperature, humidity, carbon "
        "monoxide, smoke, LPG, noise levels, light, and motion at five distinct locations: the city center, "
        "an industrial zone, a residential area, a park, and a highway bridge. The project goal is to build "
        "a reliable, scalable, and maintainable data system that ingests sensor readings as a continuous "
        "stream, processes them in near real-time, and stores them in a database accessible to front-end "
        "applications for city planners and a citizen alerting system."
    )

    # ── 2. Technical Approach ──
    pdf.small_heading("2. Technical Approach")
    pdf.body_text(
        "The system architecture follows a producer-consumer pattern built around Apache Kafka as the "
        "central message broker. A Python producer reads sensor data from a CSV dataset containing "
        "100,800 records and publishes each record as a JSON message to the Kafka topic 'sensor-data'. "
        "The device ID serves as the Kafka partition key, ensuring that all readings from a given sensor "
        "are delivered in order to the same partition. A Python consumer subscribes to this topic, "
        "deserializes the messages, and writes them to MongoDB in batches of 500 documents."
    )
    pdf.body_text(
        "During the consumption phase, each reading is enriched with two metadata fields: an ingestion "
        "timestamp recording when the data was processed, and an alerts list flagging threshold violations. "
        "The alert thresholds are configurable and currently monitor temperature (above 40 degrees Celsius), "
        "humidity (above 90 percent or below 20 percent), carbon monoxide (above 0.02 ppm), smoke "
        "(above 0.05 ppm), and noise (above 85 dB). In the test run, approximately 21 percent of all "
        "readings triggered at least one alert, primarily from the industrial zone and highway bridge sensors."
    )

    # ── 3. Technology Choices ──
    pdf.small_heading("3. Technology Choices and Justification")
    pdf.body_text(
        "Apache Kafka was chosen over Spark Streaming for several reasons. First, Kafka provides a simpler "
        "deployment model for the required producer-consumer pattern without the overhead of a full "
        "distributed computing framework. Second, Kafka persists all messages to disk, providing durability "
        "guarantees that allow consumers to replay data if needed. Third, Kafka scales horizontally by adding "
        "brokers, which aligns with the municipality's future growth plans. Spark Streaming would be more "
        "appropriate if complex windowed aggregations or machine learning inference were required at the "
        "streaming layer, but for the current use case of ingesting and forwarding sensor data, Kafka is "
        "the more efficient choice."
    )
    pdf.body_text(
        "MongoDB was selected as the storage layer because of its schema flexibility. The municipality "
        "plans to add new sensor types (CO2, fine dust) in the future, and MongoDB's document model allows "
        "new fields to be added without schema migrations. Additionally, MongoDB supports horizontal "
        "scaling through sharding, compound indexes for time-series queries, and a strong ecosystem of "
        "tooling and drivers. Alternatives such as PostgreSQL or Cassandra were considered: PostgreSQL "
        "was rejected due to its rigid schema, and Cassandra was rejected due to its more complex "
        "operational requirements for a prototype-scale project."
    )

    # ── 4. Containerization ──
    pdf.small_heading("4. Containerization and Reproducibility")
    pdf.body_text(
        "The entire pipeline is containerized using Docker Compose, defining five services: Zookeeper "
        "(cluster coordination for Kafka), Kafka (message broker), MongoDB (document store), and two "
        "custom Python containers for the producer and consumer. Health checks ensure that Kafka only "
        "starts after Zookeeper is ready, and that the producer and consumer only start after Kafka and "
        "MongoDB pass their health checks. This eliminates race conditions during startup. The system "
        "can be reproduced on any machine with Docker installed by cloning the GitHub repository and "
        "running a single command: docker-compose up --build."
    )

    # ── 5. Results ──
    pdf.small_heading("5. Results")
    pdf.body_text(
        "The pipeline successfully processed all 100,800 sensor readings. MongoDB queries confirmed "
        "correct data storage, with aggregation pipelines showing expected patterns: the industrial zone "
        "recorded the highest average temperature (21.1 degrees Celsius) and CO levels, while the park area "
        "recorded the lowest (17.2 degrees Celsius). Alert detection identified 1,580 threshold violations "
        "in the first 6,500 documents alone, demonstrating the system's capability to flag environmental "
        "anomalies for the planned citizen warning application."
    )

    # ── 6. Reflection ──
    pdf.small_heading("6. Personal Reflection")
    pdf.body_text(
        "This project deepened my understanding of stream processing architectures and the practical "
        "challenges of containerized deployments. The most significant technical challenge was ensuring "
        "the correct startup order of interdependent services. My initial health check for Zookeeper "
        "relied on netcat, which was not available in the Confluent Docker image. I solved this by "
        "switching to a bash-based TCP check, which taught me that container images often lack tools "
        "assumed to be present on standard Linux systems."
    )
    pdf.body_text(
        "Another challenge was balancing the consumer's batch size for MongoDB inserts. Smaller batches "
        "provide lower latency but higher overhead, while larger batches improve throughput at the cost "
        "of memory usage. I settled on batches of 500 documents as a reasonable trade-off for the "
        "prototype scale. In a production environment, this parameter would need to be tuned based on "
        "actual message rates and latency requirements."
    )
    pdf.body_text(
        "My problem-solving strategy for similar future projects would focus on three principles: first, "
        "start with a minimal working prototype before adding complexity; second, use health checks and "
        "dependency management to make containerized systems robust; and third, generate realistic "
        "sample data early in the process to validate the pipeline end-to-end before connecting real "
        "data sources. This project has strengthened my data engineering skills in Kafka, MongoDB, "
        "Docker, and Python, all of which are directly applicable to real-world data infrastructure roles."
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "Phase3_Abstract.pdf")
    pdf.output(path)
    print(f"Phase 3 saved: {path}")
    return path


if __name__ == "__main__":
    generate_phase1()
    generate_phase2()
    generate_phase3()
    print("\nAll portfolio PDFs generated successfully!")
