"""
Online Retail II Ingestion DAG
------------------------------
Reads online_retail_II.csv, cleans it (nulls, cancellations),
uploads to GCS, loads into BigQuery retail_raw dataset.

This dataset is messier than Walmart/Rossmann:
- Contains cancelled orders (InvoiceNo starts with 'C')
- Contains null CustomerID rows
- Contains negative quantities (returns)
We keep ALL rows in raw — cleaning happens in dbt staging.

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
DATA_DIR   = "/opt/airflow/data/raw/online_retail"
FNAME      = "online_retail_II.csv"

# ── Task 1: Validate ─────────────────────────────────────────
def validate_files():
    fpath = os.path.join(DATA_DIR, FNAME)
    if not os.path.exists(fpath):
        raise FileNotFoundError(f"Missing: {fpath}")
    if os.path.getsize(fpath) == 0:
        raise ValueError(f"Empty: {fpath}")
    logging.info(f"✅ {FNAME} present, size={os.path.getsize(fpath):,} bytes")

# ── Task 2: Upload to GCS ────────────────────────────────────
def upload_to_gcs():
    from google.cloud import storage
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(RAW_BUCKET)

    src  = os.path.join(DATA_DIR, FNAME)
    dest = f"online_retail/{FNAME}"
    blob = bucket.blob(dest)
    blob.upload_from_filename(src)
    logging.info(f"✅ Uploaded {FNAME} → gs://{RAW_BUCKET}/{dest}")

# ── Task 3: Load to BigQuery ─────────────────────────────────
def load_to_bigquery():
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    gcs_uri  = f"gs://{RAW_BUCKET}/online_retail/{FNAME}"
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.online_retail_raw"

    # All strings initially — Online Retail II has encoding issues
    # and mixed types. dbt staging will cast to proper types.
    schema = [
        bigquery.SchemaField("Invoice",     "STRING"),
        bigquery.SchemaField("StockCode",   "STRING"),
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("Quantity",    "FLOAT"),
        bigquery.SchemaField("InvoiceDate", "STRING"),  # cast to TIMESTAMP in dbt
        bigquery.SchemaField("Price",       "FLOAT"),
        bigquery.SchemaField("Customer_ID", "STRING"),  # nullable
        bigquery.SchemaField("Country",     "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format         = bigquery.SourceFormat.CSV,
        skip_leading_rows     = 1,
        schema                = schema,
        write_disposition     = bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines = True,
        allow_jagged_rows     = True,   # some rows have missing trailing fields
        encoding              = "UTF-8",
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
            "name":  "online_retail row count",
            "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.online_retail_raw`",
            "min":   500000,  # ~1M rows expected
        },
        {
            "name":  "online_retail has invoices",
            "query": f"SELECT COUNT(DISTINCT Invoice) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.online_retail_raw`",
            "min":   10000,
        },
        {
            "name":  "online_retail has countries",
            "query": f"SELECT COUNT(DISTINCT Country) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.online_retail_raw`",
            "min":   30,
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
    dag_id            = "online_retail_ingest",
    default_args      = default_args,
    description       = "Ingest Online Retail II data → GCS → BigQuery",
    schedule_interval = "@once",
    start_date        = datetime(2024, 1, 1),
    catchup           = False,
    tags              = ["ingestion", "online_retail"],
) as dag:

    t1 = PythonOperator(task_id="validate_files",     python_callable=validate_files)
    t2 = PythonOperator(task_id="upload_to_gcs",      python_callable=upload_to_gcs)
    t3 = PythonOperator(task_id="load_to_bigquery",   python_callable=load_to_bigquery)
    t4 = PythonOperator(task_id="data_quality_check", python_callable=data_quality_check)

    t1 >> t2 >> t3 >> t4