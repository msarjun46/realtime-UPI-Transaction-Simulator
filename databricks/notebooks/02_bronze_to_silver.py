# Databricks Notebook: 02_Bronze_to_Silver
# Purpose: This is the "Engine Room" of our pipeline. 
# We take messy raw logs, apply a strict financial schema, and clean the data.

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# 1. Define High-Fidelity Schema (55+ Fields)
# This schema follows NPCI/UPI industry standards.
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("bank_rrn", StringType(), True),
    StructField("payer_name", StringType(), True),
    StructField("payer_vpa", StringType(), True),
    # ... Advanced Security & Risk Fields ...
    StructField("fraud_score", DoubleType(), True),
    StructField("is_flagged", BooleanType(), True),
    StructField("device_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("streamed_at", StringType(), True)
])

# 2. Extract Data from Bronze
df_bronze = spark.readStream.table("workspace.default.upi_bronze")

# 3. TRANSFORMATION & CLEANING (Humanized Logic)
df_silver = (df_bronze
    # A. Parse the JSON payload
    .withColumn("data", F.from_json(F.col("json_payload"), schema))
    .select("data.*", "kafka_arrival_time")
    # B. Quality Check: Filter out malformed records
    .filter(F.col("transaction_id").isNotNull())
    # C. Deduplication: Ensure each transaction is unique
    .dropDuplicates(["transaction_id"])
    # D. Feature Engineering: Unified timestamp for analytics
    .withColumn("event_timestamp", F.to_timestamp(F.concat(F.col("date"), F.lit(" "), F.col("time"))))
)

# 4. Save to Silver (Cleaned Analytics Layer)
checkpoint_path = "/Volumes/workspace/default/practice/checkpoints/upi_silver"
silver_table = "workspace.default.upi_silver"

query_delta = (df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(silver_table)
)

# 5. Bridge to Local Database
# Placeholders for GitHub
KAFKA_BOOTSTRAP = "your_confluent_url"
KAFKA_API_KEY = "your_key"
KAFKA_API_SECRET = "your_secret"

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";',
}

query_kafka = (df_silver.select(F.to_json(F.struct("*")).alias("value")).writeStream
    .format("kafka")
    .options(**kafka_options)
    .option("topic", "upi_silver_detailed")
    .option("checkpointLocation", "/Volumes/workspace/default/practice/checkpoints/silver_to_kafka")
    .trigger(availableNow=True)
    .start()
)
