from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add project root to path so we can import our services
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.extract.extract_encounters import generate_encounters, save_to_bronze
from services.transform.transform_encounters import (
    get_db_engine, create_silver_table,
    read_latest_bronze_file, transform_data, load_to_silver
)
from services.sampling.sampling_encounters import (
    create_gold_table, read_from_silver,
    deterministic_sampling, load_to_gold
)

# -------------------------------------------------------
# DAG Default Arguments
# -------------------------------------------------------
default_args = {
    "owner": "zivian-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# -------------------------------------------------------
# Task Functions
# -------------------------------------------------------
def run_extraction():
    print("[DAG] Starting Encounter Extraction...")
    encounters = generate_encounters(num_records=1000)
    save_to_bronze(encounters)
    print("[DAG] Extraction complete!")


def run_transformation():
    print("[DAG] Starting Silver Layer Transformation...")
    engine = get_db_engine()
    create_silver_table(engine)
    df_raw = read_latest_bronze_file()
    df_clean = transform_data(df_raw)
    load_to_silver(df_clean, engine)
    print("[DAG] Transformation complete!")


def run_sampling():
    print("[DAG] Starting Gold Layer Sampling...")
    engine = get_db_engine()
    create_gold_table(engine)
    df_silver = read_from_silver(engine)
    df_sampled = deterministic_sampling(df_silver, sample_rate=0.10)
    load_to_gold(df_sampled, engine)
    print("[DAG] Sampling complete!")


# -------------------------------------------------------
# DAG Definition
# -------------------------------------------------------
with DAG(
    dag_id="zivian_ingestion_pipeline",
    default_args=default_args,
    description="Zivian EHR Data Ingestion Pipeline - Bronze to Silver to Gold",
    schedule_interval="0 6 * * *",  # Runs every day at 6 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["zivian", "etl", "healthcare"],
) as dag:

    # Task 1: Extract
    extract_task = PythonOperator(
        task_id="extract_encounters",
        python_callable=run_extraction,
    )

    # Task 2: Transform
    transform_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=run_transformation,
    )

    # Task 3: Sample
    sampling_task = PythonOperator(
        task_id="sample_to_gold",
        python_callable=run_sampling,
    )

    # Define order: Extract → Transform → Sample
    extract_task >> transform_task >> sampling_task