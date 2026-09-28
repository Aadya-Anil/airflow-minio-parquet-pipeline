# Airflow MinIO Parquet Pipeline

An end-to-end containerized data pipeline that ingests a local CSV file, validates it against a schema contract, transforms and cleans the data, converts it to Parquet format, and uploads it to MinIO object storage — all orchestrated by Apache Airflow running in Docker.

Demo Link : 
---

## Overview

This pipeline demonstrates a lightweight data lake ingestion pattern — the Bronze layer of a Medallion Architecture. Raw CSV data is validated, cleaned, converted to the industry-standard Parquet format, and stored in MinIO object storage for downstream use.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow 2.9.3 | Pipeline orchestration |
| MinIO | S3-compatible object storage |
| Docker + Docker Compose | Containerized environment |
| Python + Pandas | Data processing |
| PyArrow | Parquet conversion |
| PostgreSQL | Airflow metadata database |

---

## Dataset

- **Name:** 3K Conversations Dataset for ChatBot
- **Source:** Kaggle
- **Rows:** 3,725
- **Columns:** id, question, answer
- **Format:** CSV → Parquet
- **Target bucket:** `chatbot-conversations/parquet/`

---

## Project Structure
airflow-minio-parquet-pipeline/  
│
├── images/aadya-anil-kumar/project-42/  
│ ├── docker-compose.yml  
│ ├── requirements.txt  
│ └── .env.sample  
│
├── ingestion/chatbot-conversations/  
│ ├── MANIFEST.md  
│ ├── .env.sample  
│ └── config/  
│ └── schema_expected.yaml  
│
├── dags/  
│ ├── extraction/  
│ │ └── 3K Conversations Dataset for ChatBot.csv  
│ └── ingestion/  
│ └── chatbot_conversations_to_minio.py  
│
└── README.md  

---

## Pipeline Flow
file_check → validate_schema → transform → convert_to_parquet → upload_to_minio


| Task | Description |
|------|-------------|
| file_check | Verifies CSV exists at expected path |
| validate_schema | Compares CSV columns against schema_expected.yaml |
| transform | Strips whitespace, drops nulls, casts types |
| convert_to_parquet | Converts cleaned DataFrame to Parquet using PyArrow |
| upload_to_minio | Uploads Parquet file to MinIO bucket |

---

## Prerequisites

- Docker Desktop installed and running
- Python 3.10+

---

## Setup & Running

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/airflow-minio-parquet-pipeline.git
cd airflow-minio-parquet-pipeline
```

### 2. Create .env file
```bash
cd images/aadya-anil-kumar/project-42
```

Create `.env` with the following content:
```bash
AIRFLOW_UID=50000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=chatbot-conversations
MINIO_SECURE=False
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### 3. Create required folders
```bash
mkdir -p logs
mkdir -p plugins
```

### 4. Start the environment
```bash
docker compose up -d
```
Wait 3 minutes for all services to initialize.

### 5. Access Airflow UI
http://localhost:8082

- Username: `airflow`
- Password: `airflow`

### 6. Access MinIO Console
http://localhost:9001

- Username: `minioadmin`
- Password: `minioadmin`

### 7. Trigger the pipeline
1. Open Airflow UI
2. Find `chatbot_conversations_to_minio` DAG
3. Toggle ON → click ▶ to trigger manually

### 8. Verify output in MinIO
Open MinIO console → `chatbot-conversations` bucket → `parquet/` folder → confirm `chatbot_conversations.parquet` exists.

### 9. Stop the environment
```bash
docker compose down
```

---

## Schema Contract

Defined in `ingestion/chatbot-conversations/config/schema_expected.yaml`:

| Column | Type | Nullable |
|--------|------|----------|
| id | integer | false |
| question | text | false |
| answer | text | false |

DAG fails immediately if CSV does not match this schema.

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| MINIO_ENDPOINT | MinIO host | minio:9000 |
| MINIO_ACCESS_KEY | MinIO access key | minioadmin |
| MINIO_SECRET_KEY | MinIO secret key | minioadmin |
| MINIO_BUCKET | Target bucket name | chatbot-conversations |
| MINIO_SECURE | Use HTTPS | False |
| AIRFLOW_UID | Airflow user ID | 50000 |

---

## Result

After a successful run:
- ✅ 3,725 rows validated against schema contract
- ✅ Data cleaned — whitespace stripped, nulls dropped, types cast
- ✅ Parquet file generated using PyArrow
- ✅ File uploaded to MinIO at `chatbot-conversations/parquet/chatbot_conversations.parquet`

---

## Troubleshooting

**Airflow webserver not starting**
```bash
docker exec -it <webserver-container> bash -c "rm -f /opt/airflow/airflow-webserver.pid"
docker restart <webserver-container>
```

**Permission error on startup**
```bash
echo "AIRFLOW_UID=50000" >> .env
docker compose down
docker compose up -d
```

**File not found error**
Ensure CSV is placed at `dags/extraction/3K Conversations Dataset for ChatBot.csv`

**MinIO bucket not created**
The `minio-init` service creates the bucket automatically on startup. If it fails, create it manually in the MinIO console.
