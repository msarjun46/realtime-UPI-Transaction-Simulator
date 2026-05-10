# Databricks Notebook: 03_Silver_to_Gold
# Purpose: This is the "Executive Insights" layer. 
# We perform time-windowed aggregations to calculate live KPIs.

from pyspark.sql import SparkSession, functions as F

# 1. Read from Clean Silver Table
df_silver = spark.readStream.table("workspace.default.upi_silver")

# 2. STATEFUL ANALYTICS (KPI Engine)
# We use Watermarking to handle data delays automatically.
df_gold = (df_silver
    .withColumn("event_time", F.to_timestamp(F.concat(F.col("date"), F.lit(" "), F.col("time"))))
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "1 hour"), # Windowing by hour
        "transaction_status",
        "category"
    )
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_volume"),
        F.avg("amount").alias("avg_transaction_value")
    )
)

# 3. Save Aggregates to Gold Delta Table
checkpoint_path = "/Volumes/workspace/default/practice/checkpoints/upi_gold"
gold_table = "workspace.default.upi_gold"

query_gold = (df_gold.writeStream
    .format("delta")
    .outputMode("complete") # Required for aggregation outputs
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(gold_table)
)

# 4. Stream Results to Local Power BI Bridge
KAFKA_BOOTSTRAP = "your_confluent_url"
KAFKA_API_KEY = "your_key"
KAFKA_API_SECRET = "your_secret"

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";',
}

# Simplify the data structure for the local database
df_gold_kafka = df_gold.select(
    F.hour(F.col("window.start")).alias("hour"),
    "transaction_status", "total_transactions", "total_volume"
)

query_kafka = (df_gold_kafka.select(F.to_json(F.struct("*")).alias("value")).writeStream
    .format("kafka")
    .options(**kafka_options)
    .option("topic", "upi_transction_gold")
    .option("checkpointLocation", "/Volumes/workspace/default/practice/checkpoints/gold_to_kafka")
    .outputMode("complete")
    .trigger(availableNow=True)
    .start()
)
