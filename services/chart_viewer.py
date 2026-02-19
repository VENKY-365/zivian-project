import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "zivian_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres123")

def get_db_engine():
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)

def show_chart_review_report():
    engine = get_db_engine()

    print("=" * 70)
    print("ELEVATE CHART REVIEW REPORT — SELECTED PATIENT CHARTS")
    print("=" * 70)

    # Get sampled charts
    df = pd.read_sql("""
        SELECT 
            encounter_id,
            patient_id,
            physician_id,
            date_of_service,
            state,
            medications,
            clinical_notes,
            sampling_reason
        FROM gold_encounters
        WHERE is_sampled = TRUE
        ORDER BY date_of_service DESC
    """, engine)

    print(f"\nTotal Charts Selected for Review: {len(df)}")
    print(f"Sampling Method: Deterministic 10%")
    print("\n" + "-" * 70)

    # Show each chart
    for i, row in df.head(5).iterrows():
        print(f"\n📋 CHART #{i+1}")
        print(f"   Encounter ID  : {row['encounter_id']}")
        print(f"   Patient ID    : {row['patient_id']}")
        print(f"   Physician ID  : {row['physician_id']}")
        print(f"   Date of Service: {row['date_of_service']}")
        print(f"   State         : {row['state']}")
        print(f"   Medications   : {row['medications']}")
        print(f"   Clinical Notes: {row['clinical_notes']}")
        print(f"   Selected By   : {row['sampling_reason']}")
        print("-" * 70)

    print(f"\n... and {len(df) - 5} more charts ready for review")
    print("\n✅ All charts are ready to be loaded into Elevate platform!")
    print("=" * 70)

if __name__ == "__main__":
    show_chart_review_report()