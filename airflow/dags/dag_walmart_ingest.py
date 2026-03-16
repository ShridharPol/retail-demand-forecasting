"""
Walmart Ingestion DAG
---------------------
Reads 4 CSV files from local data/raw/walmart/, uploads to GCS,
then loads into BigQuery retail_raw dataset.

Task flow:
  validate_files → upload_to_gcs → load_to_bigquery → data_quality_check
"""

from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Default args applied to every task ──────────────────────
default_args = {
    "owner": "hamdan-infocom",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Constants ────────────────────────────────────────────────
PROJECT_ID  = os.environ.get("GCP_PROJECT_ID", "retail-pipeline-poc")
RAW_BUCKET  = os.environ.get("GCS_RAW_BUCKET",  "retail-pipeline-raw-retail-pipeline-poc")
BQ_DATASET  = os.environ.get("BQ_RAW_DATASET",  "retail_raw")
DATA_DIR    = "/opt/airflow/data/raw/walmart"

# Files we expect + their BigQuery table names
WALMART_FILES = {
    "train.csv":    "walmart_train",
    "test.csv":     "walmart_test",
    "stores.csv":   "walmart_stores",
    "features.csv": "walmart_features",
}

# ── Task 1: Validate files exist and are non-empty ───────────
def validate_files():
    import os
    errors = []
    for fname in WALMART_FILES:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            errors.append(f"Missing: {fpath}")
        elif os.path.getsize(fpath) == 0:
            errors.append(f"Empty: {fpath}")
    if errors:
        raise ValueError(f"File validation failed:\n" + "\n".join(errors))
    logging.info("✅ All Walmart files present and non-empty")

# ── Task 2: Upload raw files to GCS ─────────────────────────
def upload_to_gcs():
    from google.cloud import storage
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(RAW_BUCKET)

    for fname in WALMART_FILES:
        src  = os.path.join(DATA_DIR, fname)
        dest = f"walmart/{fname}"
        blob = bucket.blob(dest)
        blob.upload_from_filename(src)
        logging.info(f"✅ Uploaded {fname} → gs://{RAW_BUCKET}/{dest}")

# ── Task 3: Load from GCS into BigQuery ─────────────────────
def load_to_bigquery():
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    # Schema definitions — explicit schemas prevent type inference errors
    schemas = {
        "walmart_train": [
            bigquery.SchemaField("Store",        "INTEGER"),
            bigquery.SchemaField("Dept",         "INTEGER"),
            bigquery.SchemaField("Date",         "DATE"),
            bigquery.SchemaField("Weekly_Sales", "FLOAT"),
            bigquery.SchemaField("IsHoliday",    "BOOLEAN"),
        ],
        "walmart_stores": [
            bigquery.SchemaField("Store", "INTEGER"),
            bigquery.SchemaField("Type",  "STRING"),
            bigquery.SchemaField("Size",  "INTEGER"),
        ],
        "walmart_features": [
            bigquery.SchemaField("Store",        "STRING"),
            bigquery.SchemaField("Date",         "STRING"),
            bigquery.SchemaField("Temperature",  "STRING"),
            bigquery.SchemaField("Fuel_Price",   "STRING"),
            bigquery.SchemaField("MarkDown1",    "STRING"),
            bigquery.SchemaField("MarkDown2",    "STRING"),
            bigquery.SchemaField("MarkDown3",    "STRING"),
            bigquery.SchemaField("MarkDown4",    "STRING"),
            bigquery.SchemaField("MarkDown5",    "STRING"),
            bigquery.SchemaField("CPI",          "STRING"),
            bigquery.SchemaField("Unemployment", "STRING"),
            bigquery.SchemaField("IsHoliday",    "STRING"),
        ],
        "walmart_test": [
            bigquery.SchemaField("Store",     "INTEGER"),
            bigquery.SchemaField("Dept",      "INTEGER"),
            bigquery.SchemaField("Date",      "DATE"),
            bigquery.SchemaField("IsHoliday", "BOOLEAN"),
        ],
    }

    for fname, table_name in WALMART_FILES.items():
        gcs_uri  = f"gs://{RAW_BUCKET}/walmart/{fname}"
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format       = bigquery.SourceFormat.CSV,
            skip_leading_rows   = 1,             # skip header row
            schema              = schemas[table_name],
            write_disposition   = bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite on re-run
            allow_quoted_newlines = True,
        )

        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()  # wait for job to complete

        table = client.get_table(table_id)
        logging.info(f"✅ Loaded {table.num_rows} rows → {table_id}")

# ── Task 4: Data quality checks ──────────────────────────────
def data_quality_check():
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    checks = [
        # Check 1: train table has data
        {
            "name":  "walmart_train row count",
            "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.walmart_train`",
            "min":   400000,  # ~421k rows expected
        },
        # Check 2: no null sales values
        {
            "name":  "walmart_train null sales",
            "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.walmart_train` WHERE Weekly_Sales IS NULL",
            "max":   0,
        },
        # Check 3: stores table has 45 stores
        {
            "name":  "walmart_stores count",
            "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.walmart_stores`",
            "exact": 45,
        },
    ]

    for check in checks:
        result = client.query(check["query"]).result()
        cnt = list(result)[0].cnt

        if "min" in check and cnt < check["min"]:
            raise ValueError(f"❌ Check failed [{check['name']}]: {cnt} < {check['min']}")
        if "max" in check and cnt > check["max"]:
            raise ValueError(f"❌ Check failed [{check['name']}]: {cnt} > {check['max']}")
        if "exact" in check and cnt != check["exact"]:
            raise ValueError(f"❌ Check failed [{check['name']}]: {cnt} != {check['exact']}")

        logging.info(f"✅ Check passed [{check['name']}]: {cnt}")

# ── DAG definition ───────────────────────────────────────────
with DAG(
    dag_id            = "walmart_ingest",
    default_args      = default_args,
    description       = "Ingest Walmart store sales data → GCS → BigQuery",
    schedule_interval = "@once",   # run manually for PoC; change to @weekly for production
    start_date        = datetime(2024, 1, 1),
    catchup           = False,     # don't backfill historical runs
    tags              = ["ingestion", "walmart"],
) as dag:

    t1 = PythonOperator(task_id="validate_files",    python_callable=validate_files)
    t2 = PythonOperator(task_id="upload_to_gcs",     python_callable=upload_to_gcs)
    t3 = PythonOperator(task_id="load_to_bigquery",  python_callable=load_to_bigquery)
    t4 = PythonOperator(task_id="data_quality_check",python_callable=data_quality_check)

    t1 >> t2 >> t3 >> t4