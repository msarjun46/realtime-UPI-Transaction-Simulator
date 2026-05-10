# Project Workflow and Execution Guide

This document outlines the operational lifecycle of the pipeline, from data generation to live visualization.

## Step 1: Infrastructure Ignition
1.  **Start Local Database**: Navigate to `kafka/` and run `docker-compose up -d`.
2.  **Initialize Schema**: Run `python database/init_db.py`. This sets up the 55-column relational tables.

## Step 2: Data Generation
1.  **Launch Simulation**: Run `python producer/bulk_producer.py`. This starts the high-speed stream of 400,000 transactions to Kafka.

## Step 3: Cloud Processing (Databricks)
1.  **Run Bronze Ingestion**: Capture the raw stream.
2.  **Run Silver Transformation**: Clean and deduplicate data.
3.  **Run Gold Aggregation**: Calculate real-time KPIs.

## Step 4: Data Synchronization
1.  **Start the Bridge**: Run `python database/kafka_to_postgres.py`. This script receives clean data from the cloud and saves it locally.

## Step 5: Visualization and Insights
1.  **Refresh Dashboard**: Open Power BI and refresh the connection to the PostgreSQL database.
2.  **Verification**: Use pgAdmin to verify row counts and data integrity across the Silver and Gold tables.
