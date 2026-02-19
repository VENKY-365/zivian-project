import json
import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Database connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "zivian_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres123")

BRONZE_BASE_PATH = os.getenv("BRONZE_BASE_PATH", "./bronze")
TENANT = os.getenv("TENANT", "zivian")
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
EHR_SOURCE = os.getenv("EHR_SOURCE", "athenahealth")

def get_db_engine():
    """Create database connection."""
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)


def create_silver_table(engine):
    """Create silver table in PostgreSQL if it doesn't exist."""
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS silver_encounters (
            encounter_id        VARCHAR(20) PRIMARY KEY,
            patient_id          VARCHAR(20) NOT NULL,
            app_id              VARCHAR(20),
            physician_id        VARCHAR(20),
            date_of_service     DATE,
            state               VARCHAR(5),
            status              VARCHAR(20),
            medications         TEXT,
            clinical_notes      TEXT,
            ingestion_timestamp TIMESTAMP,
            transformed_at      TIMESTAMP DEFAULT NOW()
        );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print("[TRANSFORM] Silver table ready in PostgreSQL")


def read_latest_bronze_file():
    """Read the latest NDJSON file from Bronze layer."""
    bronze_path = os.path.join(
        BRONZE_BASE_PATH, TENANT, ENVIRONMENT, EHR_SOURCE, "encounters"
    )
    
    # Get latest folder by timestamp
    folders = sorted(os.listdir(bronze_path), reverse=True)
    latest_folder = folders[0]
    file_path = os.path.join(bronze_path, latest_folder, "encounters_raw.ndjson")
    
    print(f"[TRANSFORM] Reading Bronze file: {file_path}")
    
    records = []
    with open(file_path, "r") as f:
        for line in f:
            records.append(json.loads(line.strip()))
    
    df = pd.DataFrame(records)
    print(f"[TRANSFORM] Total raw records read: {len(df)}")
    return df


def transform_data(df):
    """Clean, validate and transform the data."""
    print("[TRANSFORM] Applying transformations...")

    # 1. Filter only completed encounters (as per technical document)
    df_completed = df[df["status"] == "completed"].copy()
    print(f"[TRANSFORM] Completed encounters: {len(df_completed)}")
    print(f"[TRANSFORM] Removed cancelled/pending: {len(df) - len(df_completed)}")

    # 2. Drop duplicates based on encounter_id
    df_completed = df_completed.drop_duplicates(subset=["encounter_id"])

    # 3. Convert date column to proper format
    df_completed["date_of_service"] = pd.to_datetime(df_completed["date_of_service"])

    # 4. Convert medications list to string for storage
    df_completed["medications"] = df_completed["medications"].apply(
        lambda x: ",".join(x) if isinstance(x, list) else x
    )

    # 5. Convert ingestion_timestamp to datetime
    df_completed["ingestion_timestamp"] = pd.to_datetime(
        df_completed["ingestion_timestamp"]
    )

    # 6. Drop rows where critical fields are missing
    df_completed = df_completed.dropna(subset=["encounter_id", "patient_id"])

    print(f"[TRANSFORM] Final clean records: {len(df_completed)}")
    return df_completed


def load_to_silver(df, engine):
    """Load transformed data into PostgreSQL Silver table."""
    print("[TRANSFORM] Loading data into Silver layer (PostgreSQL)...")

    df.to_sql(
        "silver_encounters",
        engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    print(f"[TRANSFORM] Successfully loaded {len(df)} records into silver_encounters table")


if __name__ == "__main__":
    print("=" * 60)
    print("STAGE 2 — TRANSFORM TO SILVER LAYER STARTED")
    print("=" * 60)

    # Step 1: Connect to database
    engine = get_db_engine()

    # Step 2: Create silver table
    create_silver_table(engine)

    # Step 3: Read from Bronze
    df_raw = read_latest_bronze_file()

    # Step 4: Transform
    df_clean = transform_data(df_raw)

    # Step 5: Load to Silver
    load_to_silver(df_clean, engine)

    print("\n" + "=" * 60)
    print("STAGE 2 — SILVER LAYER LOAD COMPLETED SUCCESSFULLY!")
    print("=" * 60)
