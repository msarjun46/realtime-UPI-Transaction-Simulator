# Technical Architecture: A Hybrid Streaming Model

This project implements a Hybrid Cloud-Local Medallion Architecture to handle high-velocity UPI transaction data with resilience and integrity.

## 1. Data Ingestion (Kafka)
-   **Infrastructure**: Confluent Cloud (managed Kafka).
-   **Strategy**: Kafka provides the durability and throughput needed to handle hundreds of thousands of messages. It acts as a system shock absorber, decoupling production simulators from analytical processors.

## 2. Medallion Design (Databricks)
-   **Bronze (Raw Layer)**: Serves as the system audit trail. Stores raw JSON payloads precisely as they arrived from Kafka.
-   **Silver (Cleaned Layer)**: Applies strict financial schemas, performs event-time deduplication, and engineers features like unified timestamps.
-   **Gold (Aggregated Layer)**: Utilizes stateful structured streaming to calculate hourly windowed KPIs with watermarking for late-arriving data.

## 3. Analytics Bridge (Local Sync)
-   **Engine**: Python and SQLAlchemy.
-   **Logic**: Performs idempotent upserts (ON CONFLICT) to ensure data synchronization between cloud Delta Lake and local PostgreSQL without duplication.

## 4. Technology Selection
-   **Delta Lake**: Chosen for financial ACID compliance.
-   **Stateful Streaming**: Enables live hourly updates rather than batch processing.
