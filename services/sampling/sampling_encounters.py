import os
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Database connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "zivian_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres123")


def get_db_engine():
    """Create database connection."""
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)


def create_gold_table(engine):
    """Create gold table in PostgreSQL if it doesn't exist."""
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS gold_encounters (
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
            is_sampled          BOOLEAN DEFAULT FALSE,
            sampling_reason     VARCHAR(50),
            gold_loaded_at      TIMESTAMP DEFAULT NOW()
        );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print("[SAMPLING] Gold table ready in PostgreSQL")


def read_from_silver(engine):
    """Read completed encounters from Silver layer."""
    print("[SAMPLING] Reading completed encounters from Silver layer...")
    df = pd.read_sql("SELECT * FROM silver_encounters", engine)
    print(f"[SAMPLING] Total completed encounters available: {len(df)}")
    return df


def deterministic_sampling(df, sample_rate=0.10):
    """
    Apply deterministic 10% sampling based on patient_id hash.
    
    How it works:
    - Take patient_id (e.g. PAT-0378)
    - Convert to a hash number using MD5
    - If hash % 10 == 0 → patient is selected (always same patients!)
    - This ensures same patients are always selected every run
    """
    print(f"\n[SAMPLING] Applying deterministic {int(sample_rate * 100)}% sampling...")
    print("[SAMPLING] Rule: hash(patient_id) % 10 == 0")

    def is_sampled(patient_id):
        # Convert patient_id to a number using MD5 hash
        hash_value = int(hashlib.md5(patient_id.encode()).hexdigest(), 16)
        return hash_value % 10 == 0

    # Apply sampling rule to each row
    df["is_sampled"] = df["patient_id"].apply(is_sampled)
    df["sampling_reason"] = df["is_sampled"].apply(
        lambda x: "deterministic_10pct" if x else "not_selected"
    )

    # Split into sampled and not sampled
    df_sampled = df[df["is_sampled"] == True].copy()
    df_not_sampled = df[df["is_sampled"] == False].copy()

    print(f"[SAMPLING] Total encounters processed: {len(df)}")
    print(f"[SAMPLING] Selected for chart review: {len(df_sampled)}")
    print(f"[SAMPLING] Not selected: {len(df_not_sampled)}")
    print(f"[SAMPLING] Actual sample rate: {round(len(df_sampled)/len(df)*100, 2)}%")

    return df


def load_to_gold(df, engine):
    """Load all encounters to Gold layer with sampling flags."""
    print("\n[SAMPLING] Loading data into Gold layer (PostgreSQL)...")

    df.to_sql(
        "gold_encounters",
        engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    print(f"[SAMPLING] Successfully loaded {len(df)} records into gold_encounters table")
    
    # Show summary
    sampled_count = len(df[df["is_sampled"] == True])
    print(f"\n[SAMPLING] SUMMARY:")
    print(f"           Total records in Gold: {len(df)}")
    print(f"           Selected for review:   {sampled_count}")
    print(f"           Not selected:          {len(df) - sampled_count}")


if __name__ == "__main__":
    print("=" * 60)
    print("STAGE 3 — SAMPLING & GOLD LAYER STARTED")
    print("=" * 60)

    # Step 1: Connect to database
    engine = get_db_engine()

    # Step 2: Create gold table
    create_gold_table(engine)

    # Step 3: Read from Silver
    df_silver = read_from_silver(engine)

    # Step 4: Apply deterministic sampling
    df_sampled = deterministic_sampling(df_silver, sample_rate=0.10)

    # Step 5: Load to Gold
    load_to_gold(df_sampled, engine)

    print("\n" + "=" * 60)
    print("STAGE 3 — GOLD LAYER LOAD COMPLETED SUCCESSFULLY!")
    print("=" * 60)
