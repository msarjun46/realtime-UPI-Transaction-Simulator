import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger

# Load Config
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

def get_engine(db_name=None):
    user = os.getenv("DB_USER", "postgres")
    pwd = urllib.parse.quote_plus(os.getenv("DB_PASSWORD", ""))
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    url = f"postgresql://{user}:{pwd}@{host}:{port}"
    if db_name: url += f"/{db_name}"
    return create_engine(url, isolation_level="AUTOCOMMIT")

def init():
    engine = get_engine()
    db_name = os.getenv("DB_NAME", "upi_analytics")
    
    # 1. Create DB
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {db_name}"))
        logger.info(f"🏗️ Created Database: {db_name}")

    # 2. Deploy Schema
    db_engine = get_engine(db_name)
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with db_engine.begin() as conn:
        with open(schema_path, "r") as f:
            for cmd in f.read().split(";"):
                if cmd.strip(): conn.execute(text(cmd))
    logger.success("✅ Relational Schema Deployed.")

if __name__ == "__main__":
    init()
