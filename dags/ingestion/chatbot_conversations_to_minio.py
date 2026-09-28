import os
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from io import BytesIO
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── CONFIG ────────────────────────────────────────────────────────────
DAG_ID = "chatbot_conversations_to_minio"
CSV_PATH = "/opt/airflow/dags/extraction/3K Conversations Dataset for ChatBot.csv"
SCHEMA_PATH = "/opt/airflow/ingestion/chatbot-conversations/config/schema_expected.yaml"
PARQUET_PATH = "/tmp/chatbot_conversations.parquet"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "chatbot-conversations")
MINIO_OBJECT_PATH = "chatbot-conversations/parquet/chatbot_conversations.parquet"

# ── DEFAULT ARGS ──────────────────────────────────────────────────────
default_args = {
    "owner": "aadya-anil-kumar",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# ── HELPERS ───────────────────────────────────────────────────────────
def get_minio_client():
    """Build MinIO client from environment variables."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    secure = os.environ.get("MINIO_SECURE", "False").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

# ── TASK 1: FILE CHECK ────────────────────────────────────────────────
def file_check():
    """Verify CSV file exists before proceeding."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"CSV file not found at: {CSV_PATH}. "
            "Please ensure the file is in dags/extraction/"
        )
    print(f" File found: {CSV_PATH}")

# ── TASK 2: VALIDATE SCHEMA ───────────────────────────────────────────
def validate_schema():
    """Compare CSV columns against schema_expected.yaml."""
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_columns = [col["name"] for col in schema["columns"]]

    df = pd.read_csv(CSV_PATH)

    if "Unnamed: 0" in df.columns:
        df = df.rename(columns={"Unnamed: 0": "id"})

    actual_columns = df.columns.tolist()

    missing = set(expected_columns) - set(actual_columns)
    extra = set(actual_columns) - set(expected_columns)

    if missing:
        raise ValueError(f" Schema mismatch — missing columns: {missing}")
    if extra:
        raise ValueError(f" Schema mismatch — unexpected columns: {extra}")

    for col in schema["columns"]:
        if not col["nullable"] and df[col["name"]].isnull().any():
            raise ValueError(
                f" Column '{col['name']}' has nulls but is defined as NOT NULL"
            )

    print(f" Schema validation passed. Columns: {actual_columns}")

# ── TASK 3: TRANSFORM ─────────────────────────────────────────────────
def transform():
    """Clean and normalize the CSV data."""
    df = pd.read_csv(CSV_PATH)

    if "Unnamed: 0" in df.columns:
        df = df.rename(columns={"Unnamed: 0": "id"})

    # Strip whitespace
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Drop null rows
    before = len(df)
    df = df.dropna(subset=["question", "answer"])
    after = len(df)
    if before != after:
        print(f" Dropped {before - after} rows with null values")

    # Cast types
    df["id"] = df["id"].astype(int)
    df["question"] = df["question"].astype(str)
    df["answer"] = df["answer"].astype(str)

    # Save cleaned CSV for next task
    cleaned_path = CSV_PATH.replace(".csv", "_cleaned.csv")
    df.to_csv(cleaned_path, index=False)
    print(f" Transform complete. {after} rows cleaned.")

# ── TASK 4: PARQUET CONVERSION ────────────────────────────────────────
def convert_to_parquet():
    """Convert cleaned CSV to Parquet format."""
    cleaned_path = CSV_PATH.replace(".csv", "_cleaned.csv")
    df = pd.read_csv(cleaned_path)

    # Convert to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, PARQUET_PATH)

    print(f" Parquet file written to {PARQUET_PATH}")
    print(f"   Rows: {len(df)}, Columns: {df.columns.tolist()}")

# ── TASK 5: UPLOAD TO MINIO ───────────────────────────────────────────
def upload_to_minio():
    """Upload Parquet file to MinIO bucket."""
    client = get_minio_client()

    # Create bucket if it doesn't exist
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f" Created bucket: {MINIO_BUCKET}")

    # Upload parquet file
    with open(PARQUET_PATH, "rb") as f:
        data = f.read()
        client.put_object(
            MINIO_BUCKET,
            MINIO_OBJECT_PATH,
            BytesIO(data),
            length=len(data),
            content_type="application/octet-stream"
        )

    print(f" Uploaded to MinIO: {MINIO_BUCKET}/{MINIO_OBJECT_PATH}")

# ── DAG DEFINITION ────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Ingest chatbot CSV → Parquet → MinIO",
    schedule_interval="@daily",
    catchup=False,
    tags=["chatbot", "parquet", "minio", "intern", "dhap-42"],
) as dag:

    t1 = PythonOperator(
        task_id="file_check",
        python_callable=file_check,
    )

    t2 = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    t3 = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t4 = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
    )

    t5 = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

    # Task dependencies
    t1 >> t2 >> t3 >> t4 >> t5
