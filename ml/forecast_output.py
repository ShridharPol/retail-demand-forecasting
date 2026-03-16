"""
Forecast Output Writer
-----------------------
Reads Prophet and LightGBM forecast parquet files,
combines them, adds a run_id, and writes to BigQuery
as retail_marts.mart_demand_forecast
"""

import os
import uuid
import logging
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID  = os.environ.get("GCP_PROJECT_ID", "retail-pipeline-poc")
BQ_DATASET  = "retail_marts"
BQ_TABLE    = "mart_demand_forecast"
TABLE_ID    = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

PROPHET_PATH = "ml/prophet_forecasts.parquet"
LGBM_PATH    = "ml/lgbm_forecasts.parquet"

def load_forecasts():
    dfs = []

    if os.path.exists(PROPHET_PATH):
        df = pd.read_parquet(PROPHET_PATH)
        if "dept_id" not in df.columns:
            df["dept_id"] = -1  # Prophet forecasts are store-level (aggregated)
        dfs.append(df)
        log.info(f"Loaded Prophet forecasts: {len(df):,} rows")
    else:
        log.warning(f"Prophet forecasts not found at {PROPHET_PATH}")

    if os.path.exists(LGBM_PATH):
        df = pd.read_parquet(LGBM_PATH)
        dfs.append(df)
        log.info(f"Loaded LightGBM forecasts: {len(df):,} rows")
    else:
        log.warning(f"LightGBM forecasts not found at {LGBM_PATH}")

    if not dfs:
        raise FileNotFoundError("No forecast files found. Run train_prophet.py and train_lightgbm.py first.")

    return pd.concat(dfs, ignore_index=True)

def write_to_bigquery(df):
    run_id    = str(uuid.uuid4())[:8]
    run_ts    = datetime.utcnow()

    df = df.copy()
    df["run_id"]     = run_id
    df["run_ts"]     = run_ts
    df["ds"]         = pd.to_datetime(df["ds"]).dt.date
    df["store_id"]   = df["store_id"].astype(int)
    df["dept_id"]    = df["dept_id"].astype(int)
    df["yhat"]       = df["yhat"].round(2)
    df["yhat_lower"] = df["yhat_lower"].round(2)
    df["yhat_upper"] = df["yhat_upper"].round(2)

    # Keep only needed columns
    out = df[["run_id", "run_ts", "store_id", "dept_id",
              "ds", "yhat", "yhat_lower", "yhat_upper", "model"]]

    client = bigquery.Client(project=PROJECT_ID)

    schema = [
        bigquery.SchemaField("run_id",      "STRING"),
        bigquery.SchemaField("run_ts",      "TIMESTAMP"),
        bigquery.SchemaField("store_id",    "INTEGER"),
        bigquery.SchemaField("dept_id",     "INTEGER"),
        bigquery.SchemaField("ds",          "DATE"),
        bigquery.SchemaField("yhat",        "FLOAT"),
        bigquery.SchemaField("yhat_lower",  "FLOAT"),
        bigquery.SchemaField("yhat_upper",  "FLOAT"),
        bigquery.SchemaField("model",       "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema            = schema,
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(out, TABLE_ID, job_config=job_config)
    job.result()

    log.info(f"✅ Written {len(out):,} forecast rows → {TABLE_ID}")
    log.info(f"   Run ID: {run_id} | Models: {out['model'].unique().tolist()}")

if __name__ == "__main__":
    df = load_forecasts()
    write_to_bigquery(df)