"""
Rossmann Ingestion DAG
----------------------
Reads 3 CSV files from local data/raw/rossmann/, uploads to GCS,
then loads into BigQuery retail_raw dataset.

Task flow:
  validate_files → upload_to_gcs → load_to_bigquery → data_quality_check
"""

from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "hamdan-infocom",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "retail-pipeline-poc")
RAW_BUCKET = os.environ.get("GCS_RAW_BUCKET",  "retail-pipeline-raw-retail-pipeline-poc")
BQ_DATASET = os.environ.get("BQ_RAW_DATASET",  "retail_raw")
DATA_DIR   = "/opt/airflow/data/raw/rossmann"

ROSSMANN_FILES = {
    "train.csv": "rossmann_train",
    "test.csv":  "rossmann_test",
    "store.csv": "rossmann_stores",
}

# ── Task 1: Validate ─────────────────────────────────────────
def validate_files():
    errors = []
    for fname in ROSSMANN_FILES:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            errors.append(f"Missing: {fpath}")
        elif os.path.getsize(fpath) == 0:
            errors.append(f"Empty: {fpath}")
    if errors:
        raise ValueError("File validation failed:\n" + "\n".join(errors))
    logging.info("✅ All Rossmann files present and non-empty")

# ── Task 2: Upload to GCS ────────────────────────────────────
def upload_to_gcs():
    from google.cloud import storage
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(RAW_BUCKET)

    for fname in ROSSMANN_FILES:
        src  = os.path.join(DATA_DIR, fname)
        dest = f"rossmann/{fname}"
        blob = bucket.blob(dest)
        blob.upload_from_filename(src)
        logging.info(f"✅ Uploaded {fname} → gs://{RAW_BUCKET}/{dest}")

# ── Task 3: Load to BigQuery ─────────────────────────────────
def load_to_bigquery():
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    schemas = {
        "rossmann_train": [
            bigquery.SchemaField("Store",         "INTEGER"),
            bigquery.SchemaField("DayOfWeek",     "INTEGER"),
            bigquery.SchemaField("Date",          "DATE"),
            bigquery.SchemaField("Sales",         "INTEGER"),
            bigquery.SchemaField("Customers",     "INTEGER"),
            bigquery.SchemaField("Open",          "INTEGER"),
            bigquery.SchemaField("Promo",         "INTEGER"),
            bigquery.SchemaField("StateHoliday",  "STRING"),
            bigquery.SchemaField("SchoolHoliday", "INTEGER"),
        ],
        "rossmann_test": [
            bigquery.SchemaField("Id",            "INTEGER"),
            bigquery.SchemaField("Store",         "INTEGER"),
            bigquery.SchemaField("DayOfWeek",     "INTEGER"),
            bigquery.SchemaField("Date",          "DATE"),
            bigquery.SchemaField("Open",          "FLOAT"),
            bigquery.SchemaField("Promo",         "INTEGER"),
            bigquery.SchemaField("StateHoliday",  "STRING"),
            bigquery.SchemaField("SchoolHoliday", "INTEGER"),
        ],
        "rossmann_stores": [
            bigquery.SchemaField("Store",                    "INTEGER"),
            bigquery.SchemaField("StoreType",                "STRING"),
            bigquery.SchemaField("Assortment",               "STRING"),
            bigquery.SchemaField("CompetitionDistance",      "FLOAT"),
            bigquery.SchemaField("CompetitionOpenSinceMonth","FLOAT"),
            bigquery.SchemaField("CompetitionOpenSinceYear", "FLOAT"),
            bigquery.SchemaField("Promo2",                   "INTEGER"),
            bigquery.SchemaField("Promo2SinceWeek",          "FLOAT"),
            bigquery.SchemaField("Promo2SinceYear",          "FLOAT"),
            bigquery.SchemaField("PromoInterval",            "STRING"),
        ],
    }

    for fname, table_name in ROSSMANN_FILES.items():
        gcs_uri  = f"gs://{RAW_BUCKET}/rossmann/{fname}"
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format       = bigquery.SourceFormat.CSV,
            skip_leading_rows   = 1,
            schema              = schemas[table_name],
            write_disposition   = bigquery.WriteDisposition.WRITE_TRUNCATE,
            allow_quoted_newlines = True,
        )

        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()

        table = client.get_table(table_id)
        logging.info(f"✅ Loaded {table.num_rows} rows → {table_id}")

# ── Task 4: Data quality checks ──────────────────────────────
def data_quality_check():
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    checks = [
        {
            "name":  "rossmann_train row count",
            "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.rossmann_train`",
            "min":   1000000,  # ~1.01M rows expected
        },
        {
            "name":  "rossmann_train null sales",
            "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.rossmann_train` WHERE Sales IS NULL",
            "max":   0,
        },
        {
            "name":  "rossmann_stores count",
            "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.rossmann_stores`",
            "exact": 1115,
        },
    ]

    for check in checks:
        result = client.query(check["query"]).result()
        cnt = list(result)[0].cnt

        if "min"   in check and cnt < check["min"]:
            raise ValueError(f"❌ [{check['name']}]: {cnt} < {check['min']}")
        if "max"   in check and cnt > check["max"]:
            raise ValueError(f"❌ [{check['name']}]: {cnt} > {check['max']}")
        if "exact" in check and cnt != check["exact"]:
            raise ValueError(f"❌ [{check['name']}]: {cnt} != {check['exact']}")

        logging.info(f"✅ Check passed [{check['name']}]: {cnt}")

# ── DAG definition ───────────────────────────────────────────
with DAG(
    dag_id            = "rossmann_ingest",
    default_args      = default_args,
    description       = "Ingest Rossmann store sales data → GCS → BigQuery",
    schedule_interval = "@once",
    start_date        = datetime(2024, 1, 1),
    catchup           = False,
    tags              = ["ingestion", "rossmann"],
) as dag:

    t1 = PythonOperator(task_id="validate_files",     python_callable=validate_files)
    t2 = PythonOperator(task_id="upload_to_gcs",      python_callable=upload_to_gcs)
    t3 = PythonOperator(task_id="load_to_bigquery",   python_callable=load_to_bigquery)
    t4 = PythonOperator(task_id="data_quality_check", python_callable=data_quality_check)

    t1 >> t2 >> t3 >> t4