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

## Technical Stack
-   **Streaming**: Apache Kafka (Confluent Cloud)
-   **Processing**: PySpark / Spark Structured Streaming
-   **Storage**: Delta Lake (Databricks)
-   **Database**: PostgreSQL (Relational Model)
-   **Environment**: Python 3.14+, Docker

---

## Medallion Architecture
The project utilizes the Medallion Pattern to ensure a single source of truth:
-   **Bronze**: Raw data preservation for auditing and re-processing.
-   **Silver**: Data cleaning, deduplication, and strict schema enforcement.
-   **Gold**: High-level business KPI aggregations and windowed analytics.

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
