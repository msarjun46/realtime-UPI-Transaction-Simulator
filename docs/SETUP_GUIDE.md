# Step-by-Step Setup Guide

This guide provides a comprehensive path to getting the UPI Transaction Simulator running on your local machine and in the cloud.

## Phase 1: Pre-requisites
Ensure you have the following installed:
1.  **Python 3.14+**
2.  **Docker Desktop**
3.  **Confluent Cloud Account**
4.  **Databricks Environment**

## Phase 2: Environment Configuration
1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/mdsarjun46/upi_transaction_simulator.git
    cd upi_transaction_simulator
    ```
2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Setup Secrets**:
    -   Copy `config/.env.example` to `config/.env`.
    -   Fill in your Kafka credentials and PostgreSQL password.

## Phase 3: Infrastructure
1.  **Database**: Run `docker-compose -f kafka/docker-compose.yml up -d`.
2.  **Schema**: Run `python database/init_db.py`.

## Phase 4: Cloud Setup
1.  **Notebooks**: Import the files from `databricks/notebooks/` into Databricks.
2.  **Credentials**: Update the placeholder variables in the notebooks with your Confluent keys.

## Phase 5: Execution
1.  **Stream**: Run `python producer/bulk_producer.py`.
2.  **Process**: Run Databricks notebooks (01 -> 02 -> 03).
3.  **Sync**: Run `python database/kafka_to_postgres.py`.
