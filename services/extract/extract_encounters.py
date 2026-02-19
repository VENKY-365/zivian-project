import json
import os
import uuid
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv
import random

# Load environment variables
load_dotenv()

fake = Faker()
random.seed(42)  # Fixed seed = deterministic data every time

BRONZE_BASE_PATH = os.getenv("BRONZE_BASE_PATH", "./bronze")
TENANT = os.getenv("TENANT", "zivian")
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
EHR_SOURCE = os.getenv("EHR_SOURCE", "athenahealth")

def generate_encounters(num_records=1000):
    """
    Simulates pulling encounter data from AthenaHealth API.
    In real project this would be an actual API call with OAuth2.
    """
    print(f"[EXTRACT] Generating {num_records} patient encounters from {EHR_SOURCE}...")
    
    encounters = []
    statuses = ["completed", "completed", "completed", "cancelled", "pending"]
    states = ["CA", "TX", "NY", "FL", "IL", "WA", "GA", "AZ"]
    medications = [
        "NDC-0069-0001", "NDC-0069-0002", "NDC-0069-0003",
        "NDC-0069-0004", "NDC-0069-0005"
    ]

    for i in range(1, num_records + 1):
        encounter = {
            "encounter_id": f"ENC-{str(i).zfill(5)}",
            "patient_id": f"PAT-{str(random.randint(1, 500)).zfill(4)}",
            "app_id": f"APP-{str(random.randint(1, 50)).zfill(3)}",
            "physician_id": f"PHY-{str(random.randint(1, 100)).zfill(3)}",
            "date_of_service": str(fake.date_between(
                start_date="-30d", end_date="today"
            )),
            "state": random.choice(states),
            "status": random.choice(statuses),
            "medications": random.sample(medications, k=random.randint(1, 3)),
            "clinical_notes": f"Patient visit note {i} - {fake.sentence()}",
            "ingestion_timestamp": datetime.utcnow().isoformat()
        }
        encounters.append(encounter)

    print(f"[EXTRACT] Successfully generated {len(encounters)} encounters")
    return encounters


def save_to_bronze(encounters):
    """
    Saves raw encounter data to Bronze layer.
    Simulates writing to Azure Blob Storage.
    Directory structure matches the technical document exactly.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    
    # Build folder path exactly as per technical document
    bronze_path = os.path.join(
        BRONZE_BASE_PATH,
        TENANT,
        ENVIRONMENT,
        EHR_SOURCE,
        "encounters",
        timestamp
    )
    
    # Create folder
    os.makedirs(bronze_path, exist_ok=True)
    
    # Save as NDJSON (one JSON record per line — FHIR standard format)
    file_path = os.path.join(bronze_path, "encounters_raw.ndjson")
    
    with open(file_path, "w") as f:
        for encounter in encounters:
            f.write(json.dumps(encounter) + "\n")
    
    print(f"[EXTRACT] Raw data saved to Bronze layer:")
    print(f"          Path: {file_path}")
    print(f"          Total records: {len(encounters)}")
    return file_path


if __name__ == "__main__":
    print("=" * 60)
    print("STAGE 1 — ENCOUNTER EXTRACTION STARTED")
    print("=" * 60)
    
    # Step 1: Generate/Extract encounters
    encounters = generate_encounters(num_records=1000)
    
    # Step 2: Save to Bronze layer
    bronze_file = save_to_bronze(encounters)
    
    print("\n" + "=" * 60)
    print("STAGE 1 — EXTRACTION COMPLETED SUCCESSFULLY!")
    print("=" * 60)
