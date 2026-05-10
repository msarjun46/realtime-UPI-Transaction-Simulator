import time
import os
import json
from confluent_kafka import Producer
from dotenv import load_dotenv
from data_generator import generate_transaction
from loguru import logger

# Load environment
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
}

def run_bulk_producer(count=100000):
    """Generates and streams a large volume of data to Kafka."""
    producer = Producer(KAFKA_CONFIG)
    logger.info(f"🚀 Starting Bulk Simulation: {count} records...")
    
    for i in range(count):
        record = generate_transaction()
        producer.produce(
            "upi_transactions", 
            key=record['transaction_id'], 
            value=json.dumps(record)
        )
        if i % 10000 == 0:
            logger.info(f"📤 Sent {i} records...")
            producer.flush()
            
    producer.flush()
    logger.success("✅ Bulk Generation Complete!")

if __name__ == "__main__":
    run_bulk_producer()
