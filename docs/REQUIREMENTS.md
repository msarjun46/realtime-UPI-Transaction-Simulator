# Requirements Definition

This document outlines the core objectives and technical specifications for the Real-Time UPI Transaction Simulator.

---

## 1. Project Objective
The primary goal is to build a resilient, real-time data engineering pipeline that simulates a high-velocity UPI ecosystem. The system must process transaction events from generation to visualization with absolute data integrity.

## 2. Functional Requirements
The system must support the following business capabilities:
-   **Real-Time Streaming**: Capture and buffer up to 400,000 transactions using a distributed messaging system.
-   **Medallion Processing**: Implement a 3-layer (Bronze, Silver, Gold) transformation logic for data refining.
-   **Fraud Detection**: Calculate a real-time risk score (0-1) and flag high-risk transactions (>0.85).
-   **Geo-Spatial Tracking**: Capture and map latitude/longitude coordinates for national spend analysis.
-   **Analytical Serving**: Synchronize cleaned cloud data with a local relational database for persistent storage.

## 3. Technical Requirements (The Stack)
-   **Language**: Python 3.14+ (for simulation and bridge logic).
-   **Messaging**: Apache Kafka via Confluent Cloud (for distributed streaming).
-   **Processing**: PySpark / Spark Structured Streaming via Databricks (for scalable ETL).
-   **Database**: PostgreSQL 15+ via Docker (for local analytical serving).
-   **Visualization**: Power BI Desktop (for real-time dashboarding).

## 4. Core Dependencies
The following libraries are required for the local environment:
-   `confluent-kafka`: Kafka client for Python.
-   `sqlalchemy` & `psycopg2-binary`: Relational database mapping.
-   `python-dotenv`: Secure environment variable management.
-   `loguru`: Professional-grade logging.
-   `pyspark`: Spark logic and schema definitions.
