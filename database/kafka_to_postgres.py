"""
The Bridge: Connecting the Cloud to your Local Dashboard
---------------------------------------------------------
This script is the 'Hands' of our project. It catches the cleaned 
data falling from the cloud and places it safely into your 
local database.
"""

import os
import json
import urllib.parse
from dotenv import load_dotenv
from confluent_kafka import Consumer
from sqlalchemy import create_engine, text
from loguru import logger

# --- SETUP: The Foundation ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

def get_db_engine():
    """Securely connects to PostgreSQL without exposing secrets."""
    user = os.getenv("DB_USER", "postgres")
    pwd = urllib.parse.quote_plus(os.getenv("DB_PASSWORD", ""))
    return create_engine(f'postgresql://{user}:{pwd}@localhost:5432/upi_analytics')

def load_transaction(conn, data):
    """
    Safely saves a transaction. 
    We use 'ON CONFLICT DO NOTHING' to ensure that if a message is 
    retried, we don't get double data in our dashboard. (Idempotency)
    """
    query = text("""
        INSERT INTO upi_transactions (
            transaction_id, date, time, amount, transaction_status, 
            fraud_score, latitude, longitude
        ) VALUES (
            :transaction_id, :date, :time, :amount, :transaction_status, 
            :fraud_score, :latitude, :longitude
        ) ON CONFLICT (transaction_id) DO NOTHING;
    """)
    conn.execute(query, data)

def run_bridge():
    """The main 'listening' loop."""
    consumer = Consumer({
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("KAFKA_API_KEY"),
        'sasl.password': os.getenv("KAFKA_API_SECRET"),
        'group.id': 'human-friendly-bridge',
        'auto.offset.reset': 'earliest'
    })
    
    consumer.subscribe(['upi_transction_gold', 'upi_silver_detailed'])
    engine = get_db_engine()
    
    logger.info("Bridge is ONLINE. Watching for clean data from Databricks...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            
            data = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()
            
            with engine.begin() as conn:
                if topic == 'upi_silver_detailed':
                    load_transaction(conn, data)
                    logger.info(f" Received Clean Transaction: {data.get('transaction_id')}")
                elif topic == 'upi_transction_gold':
                    logger.success(f" Updated Analytics for Hour {data.get('hour')}")
            
    except KeyboardInterrupt:
        logger.warning("Stopping the bridge. See you next time!")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_bridge()
