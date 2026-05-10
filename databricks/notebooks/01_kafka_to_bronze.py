# Databricks Notebook: 01_Kafka_to_Bronze
# Purpose: This is the entry point of our Medallion Architecture. 
# It captures raw UPI transaction streams from Kafka and saves them 
# into a "Bronze" Delta table for historical auditing.

from pyspark.sql import SparkSession, functions as F

# --- SECURITY BEST PRACTICE ---
# In a professional environment, never hardcode API keys.
# Use Databricks Secrets or environment variables.
KAFKA_BOOTSTRAP = "your_confluent_url_here"
KAFKA_API_KEY = "your_api_key_here"
KAFKA_API_SECRET = "your_api_secret_here"

# Kafka configuration for Confluent Cloud
kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";',
    "subscribe": "upi_transactions",
    "startingOffsets": "earliest"
}

# 1. Start the Stream: Connecting to the live "Firehose" of data
df_stream_raw = (spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .option("failOnDataLoss", "false") # Continue even if Kafka rotates old records
    .load()
)

# 2. Bronze Layer: We keep the data RAW. 
# We only capture the message body and the arrival time.
df_bronze = (df_stream_raw
    .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_arrival_time")
)

# 3. Persistent Storage (Delta Lake)
checkpoint_path = "/Volumes/workspace/default/practice/checkpoints/upi_bronze"
bronze_table = "workspace.default.upi_bronze"

query = (df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True) # Process in efficient batches
    .toTable(bronze_table)
)
