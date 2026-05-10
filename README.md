# Real-Time UPI Transaction Simulator: Medallion Pipeline

### The Vision
In the fast-paced world of Indian Fintech, processing millions of transactions with zero data loss is critical. This project simulates a Production-Grade Financial Engine, demonstrating how a modern Data Engineer handles high-velocity streams while ensuring absolute data integrity.

---

## The Problem Statement
In a real-time UPI environment, every transaction must be verified for:
1.  **Risk Assessment**: Real-time fraud scoring.
2.  **Spatial Context**: Geo-location tracking for spending patterns.
3.  **Bank Connectivity**: Response code analysis and failure handling.

This pipeline captures this journey, transforms it into clean insights, and serves it to an analytical dashboard in real-time.

---

## Project Documentation
-   **[Architecture & Requirements](docs/ARCHITECTURE.md)**: Technical design, data flow, and project specifications.
-   **[Operational SOP](docs/SOP.md)**: Professional standards and maintenance.
-   **[Step-by-Step Setup](docs/SETUP_GUIDE.md)**: Installation and execution manual.

---

## Technical Stack
-   **Streaming Engine**: Apache Kafka (Confluent Cloud)
-   **Processing Layer**: PySpark / Spark Structured Streaming (Databricks)
-   **Cloud Storage**: Delta Lake (ACID compliant)
-   **Analytical Database**: **PostgreSQL** (Relational Serving Layer)
-   **Visualization**: **Power BI** (Real-Time Dashboards)
-   **Orchestration**: Python 3.14+, Docker

---

## Medallion Architecture (The Elite 55-Field Schema)
The project utilizes the Medallion Pattern to process 55+ high-fidelity analytical fields:
-   **Bronze**: The "Memory"—Saving raw JSON logs exactly as they arrive from Kafka.
-   **Silver**: The "Filter"—Where we apply strict schemas, deduplicate data, and calculate **Fraud Scores** and **Geo-Location** features.
-   **Gold**: The "Wisdom"—Where we perform windowed aggregations for live Hourly KPIs.

---

## Execution Overview
1.  **Initialize Database**: `python database/init_db.py`
2.  **Start Stream**: `python producer/bulk_producer.py`
3.  **Cloud Processing**: Execute the Databricks notebooks (01 -> 02 -> 03).
4.  **Local Sync**: Run `python database/kafka_to_postgres.py`

---

## Author
**Mohamed Sarjun J**  
**Email**: sarjunmd1204@gmail.com  
**LinkedIn**: [mdsarjun2004](https://www.linkedin.com/in/mdsarjun2004)  
*Data Engineering Portfolio Project*
