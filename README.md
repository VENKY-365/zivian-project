# 🏥 Zivian — Cloud-Native EHR Data Ingestion Pipeline

> A production-grade, Bronze → Silver → Gold data engineering platform built for Zivian's Elevate Chart Review system. Designed to ingest, transform, and sample patient encounter data from AthenaHealth EHR for supervising physician review.

---

## 📌 Project Overview

Zivian required a modern, scalable, and secure data ingestion platform to support their **Elevate** platform — a physician chart review system used for regulatory compliance and quality assurance in healthcare.

This project replaces legacy ETL approaches with a **container-first, orchestration-driven architecture** that supports:
- Multi-stage data ingestion (Bronze → Silver → Gold)
- Deterministic 10% encounter sampling for regulatory compliance
- Full auditability and data lineage
- Automated daily pipeline execution

---

## 🏗️ Architecture

```
AthenaHealth EHR (Source)
         │
         ▼
┌─────────────────┐
│  BRONZE LAYER   │  ← Raw NDJSON files (Azure Blob Storage / Local)
│  Immutable,     │    Append-only, exact copy of source data
│  Append-only    │    Directory: /bronze/{tenant}/{env}/{ehr}/{entity}/{timestamp}/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │  ← PostgreSQL (Staging)
│  Cleaned &      │    Filtered, validated, deduplicated
│  Validated      │    Only completed encounters
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   GOLD LAYER    │  ← PostgreSQL (Refined)
│  Business-Ready │    10% deterministic sampling applied
│  + Sampled      │    Flagged for physician chart review
└────────┬────────┘
         │
         ▼
  Elevate Platform
  (Physician Chart Review)
```

---

## 🔄 Pipeline Stages

### Stage 1 — Encounter Extraction (Bronze Layer)
- Simulates AthenaHealth API extraction using OAuth2
- Generates 1,000 patient encounter records
- Stores raw data as **NDJSON** (FHIR standard format)
- Directory structure supports full audit trail and replay

### Stage 2 — Data Transformation (Silver Layer)
- Reads raw data from Bronze layer
- Filters only **completed** encounters (removes cancelled/pending)
- Validates schema, enforces data types
- Deduplicates records by `encounter_id`
- Loads **584 clean records** into PostgreSQL Silver table

### Stage 3 — Sampling & Gold Layer
- Reads completed encounters from Silver layer
- Applies **deterministic 10% sampling** using MD5 hash of `patient_id`
- Same patients are always selected — ensures longitudinal consistency
- Flags **59 encounters** as `is_sampled = TRUE` for chart review
- Loads all records into PostgreSQL Gold table with sampling metadata

---

## 🧠 Deterministic Sampling — How It Works

```python
def is_sampled(patient_id):
    hash_value = int(hashlib.md5(patient_id.encode()).hexdigest(), 16)
    return hash_value % 10 == 0
```

| Property | Detail |
|----------|--------|
| Method | MD5 Hash % 10 |
| Sample Rate | 10% |
| Consistency | Same patients always selected |
| Auditability | Fully explainable to regulators |
| Replay-safe | No drift on re-runs |

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Language | Python 3.8+ | Core pipeline logic |
| Orchestration | Apache Airflow 2.8 | DAG scheduling & monitoring |
| Compute | Docker + Docker Compose | Containerized execution |
| Bronze Storage | Local NDJSON / Azure Blob | Raw immutable data store |
| Silver/Gold DB | PostgreSQL 15 | Staging & refined data |
| Data Processing | Pandas + SQLAlchemy | Transform & load |
| Dashboard | Flask + HTML/CSS | Elevate chart review UI |
| CI/CD | GitHub | Version control & deployment |

---

## 📁 Project Structure

```
zivian-project/
├── bronze/                          # Raw data storage (Azure Blob simulation)
│   └── zivian/dev/athenahealth/
│       └── encounters/{timestamp}/
│           └── encounters_raw.ndjson
├── dags/
│   └── zivian_ingestion_dag.py      # Airflow DAG definition
├── services/
│   ├── extract/
│   │   └── extract_encounters.py    # Stage 1: EHR data extraction
│   ├── transform/
│   │   └── transform_encounters.py  # Stage 2: Silver layer transformation
│   ├── sampling/
│   │   └── sampling_encounters.py   # Stage 3: Gold layer + 10% sampling
│   ├── dashboard.py                 # Flask web dashboard
│   └── chart_viewer.py             # CLI chart review report
├── docs/
│   └── index.html                   # GitHub Pages dashboard
├── docker-compose.yml               # Full stack container setup
├── Dockerfile                       # Custom Airflow image
├── requirements.txt                 # Python dependencies
└── .env                            # Environment configuration
```

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop installed
- Python 3.8+
- PostgreSQL 15+

### Step 1 — Clone the Repository
```bash
git clone https://github.com/VENKY-365/zivian-project.git
cd zivian-project
```

### Step 2 — Start the Stack
```bash
docker-compose up -d
```

### Step 3 — Access Airflow
Open your browser: **http://localhost:8080**
- Username: `admin`
- Password: `admin`

### Step 4 — Trigger the Pipeline
- Find `zivian_ingestion_pipeline` DAG
- Click ▶ to trigger
- Watch Extract → Transform → Sample run automatically!

### Step 5 — View the Dashboard
```bash
pip install -r requirements.txt
python services/dashboard.py
```
Open: **http://localhost:5000**

---

## 📊 Pipeline Results

| Metric | Value |
|--------|-------|
| Raw Encounters Extracted | 1,000 |
| Completed Encounters (Silver) | 584 |
| Cancelled / Pending (Excluded) | 416 |
| Charts Selected for Review (Gold) | 59 |
| Sample Rate | 10% (Deterministic) |
| Pipeline Schedule | Daily at 6:00 AM UTC |

---

## 🔐 Security & Compliance

- No PHI written to logs or Airflow metadata
- Secrets managed via `.env` file (Azure Key Vault in production)
- Encryption in transit and at rest
- Full audit trail via Bronze layer immutability
- Deterministic sampling ensures regulatory explainability

---

## 🌐 Live Dashboard

View the live Elevate Chart Review Dashboard:
**https://venky-365.github.io/zivian-project**

---

## 📋 Airflow DAG

```
zivian_ingestion_pipeline
│
├── extract_encounters      (Stage 1 — Bronze Layer)
│         │
│         ▼
├── transform_to_silver     (Stage 2 — Silver Layer)
│         │
│         ▼
└── sample_to_gold          (Stage 3 — Gold Layer)
```

- Schedule: `0 6 * * *` (Every day at 6 AM)
- Retries: 1 (with 2-minute delay)
- Tags: `zivian`, `etl`, `healthcare`

---

## 👨‍💻 Author

**Venkatesh** — Data Engineer
GitHub: [@VENKY-365](https://github.com/VENKY-365)

---

*Built as a POC/Demo for Zivian's Cloud-Native Data Ingestion Platform*
