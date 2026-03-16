"""
Prophet Per-Store Forecasting
-----------------------------
- Reads mart_demand_features from BigQuery
- Trains one Prophet model per store (aggregated across all depts)
- Logs parameters, metrics, and model to MLflow
- Writes forecasts to a local parquet file for forecast_output.py
- Metric: WMAE (Weighted Mean Absolute Error) — Walmart competition metric
"""

import os
import logging
import warnings
import pandas as pd
import numpy as np
import mlflow
import mlflow.prophet
from prophet import Prophet
from google.cloud import bigquery

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
PROJECT_ID         = os.environ.get("GCP_PROJECT_ID", "retail-pipeline-poc")
BQ_DATASET         = "retail_marts"
TABLE              = "mart_demand_features"
MLFLOW_URI         = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
FORECAST_HORIZON   = 12   # weeks ahead
MAX_STORES         = 10   # limit for PoC — set to None for all 45 stores
OUTPUT_PATH        = "ml/prophet_forecasts.parquet"

# ── MLflow setup ─────────────────────────────────────────────
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("prophet_per_store")

# ── Load data from BigQuery ──────────────────────────────────
def load_data():
    client = bigquery.Client(project=PROJECT_ID)
    query  = f"""
        SELECT
            store_id,
            dept_id,
            sale_date,
            weekly_sales,
            is_holiday
        FROM `{PROJECT_ID}.{BQ_DATASET}.{TABLE}`
        WHERE weekly_sales > 0
        ORDER BY store_id, sale_date
    """
    log.info("Loading data from BigQuery...")
    df = client.query(query).to_dataframe()
    log.info(f"Loaded {len(df):,} rows for {df['store_id'].nunique()} stores")
    return df

# ── WMAE metric ──────────────────────────────────────────────
def wmae(actual, predicted, weights):
    """Weighted Mean Absolute Error — official Walmart competition metric"""
    return np.sum(weights * np.abs(actual - predicted)) / np.sum(weights)

# ── Train Prophet for one store ──────────────────────────────
def train_store(store_id, df_store):
    # Aggregate across all depts for store-level forecast
    df_agg = df_store.groupby("sale_date").agg(
        y         = ("weekly_sales", "sum"),
        is_holiday= ("is_holiday", "max")
    ).reset_index().rename(columns={"sale_date": "ds"})

    df_agg["ds"] = pd.to_datetime(df_agg["ds"])
    df_agg = df_agg.sort_values("ds").reset_index(drop=True)

    if len(df_agg) < 52:
        log.warning(f"Store {store_id}: not enough data ({len(df_agg)} weeks), skipping")
        return None, None

    # Train/test split — last 12 weeks as holdout
    train = df_agg.iloc[:-FORECAST_HORIZON]
    test  = df_agg.iloc[-FORECAST_HORIZON:]

    # Build holiday dataframe for Prophet
    holidays = df_agg[df_agg["is_holiday"] == True][["ds"]].copy()
    holidays["holiday"] = "retail_holiday"
    holidays["lower_window"] = 0
    holidays["upper_window"] = 1

    # Prophet parameters
    params = {
        "yearly_seasonality" : True,
        "weekly_seasonality" : True,
        "daily_seasonality"  : False,
        "seasonality_mode"   : "multiplicative",  # better for retail (scales with trend)
        "changepoint_prior_scale": 0.05,           # controls trend flexibility
    }

    model = Prophet(holidays=holidays, **params)
    model.fit(train[["ds", "y"]])

    # Predict on full history + forecast horizon
    future   = model.make_future_dataframe(periods=FORECAST_HORIZON, freq="W")
    forecast = model.predict(future)
    forecast["ds"] = pd.to_datetime(forecast["ds"])

    # Evaluate on holdout — match by position (last FORECAST_HORIZON rows of training data)
    test_dates   = pd.to_datetime(test["ds"].values)
    pred_df      = forecast[forecast["ds"].isin(test_dates)].copy()

    if len(pred_df) < len(test):
        # fallback: slice by position
        pred_test = forecast.iloc[len(train):len(train) + FORECAST_HORIZON]["yhat"].values
    else:
        pred_test = pred_df["yhat"].values

    min_len   = min(len(pred_test), len(test))
    pred_test = pred_test[:min_len]
    actual    = test["y"].values[:min_len]
    weights   = np.where(test["is_holiday"].values[:min_len], 5, 1)
    score     = wmae(actual, pred_test, weights)

    # Build forecast output — future periods only
    future_only = forecast.tail(FORECAST_HORIZON)[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    future_only["store_id"] = store_id
    future_only["model"]    = "prophet"

    return score, future_only

# ── Main ─────────────────────────────────────────────────────
def main():
    df = load_data()
    stores = sorted(df["store_id"].unique())
    if MAX_STORES:
        stores = stores[:MAX_STORES]
        log.info(f"PoC mode: training on {MAX_STORES} stores")

    all_forecasts = []
    all_scores    = []

    for store_id in stores:
        df_store = df[df["store_id"] == store_id]

        with mlflow.start_run(run_name=f"prophet_store_{store_id}"):
            mlflow.log_param("store_id",             store_id)
            mlflow.log_param("forecast_horizon",     FORECAST_HORIZON)
            mlflow.log_param("seasonality_mode",     "multiplicative")
            mlflow.log_param("changepoint_prior",    0.05)
            mlflow.log_param("train_weeks",          len(df_store["sale_date"].unique()))

            score, forecast_df = train_store(store_id, df_store)

            if score is None:
                mlflow.log_metric("wmae", -1)
                continue

            mlflow.log_metric("wmae", round(score, 2))
            log.info(f"Store {store_id}: WMAE = {score:.2f}")

            all_forecasts.append(forecast_df)
            all_scores.append({"store_id": store_id, "wmae": score})

    # Save forecasts
    if all_forecasts:
        pd.concat(all_forecasts).to_parquet(OUTPUT_PATH, index=False)
        log.info(f"✅ Saved {len(all_forecasts)} store forecasts → {OUTPUT_PATH}")

    # Summary
    scores_df = pd.DataFrame(all_scores)
    log.info(f"\n📊 Prophet Summary:")
    log.info(f"   Stores trained : {len(scores_df)}")
    log.info(f"   Mean WMAE      : {scores_df['wmae'].mean():.2f}")
    log.info(f"   Best store     : {scores_df.loc[scores_df['wmae'].idxmin(), 'store_id']} ({scores_df['wmae'].min():.2f})")
    log.info(f"   Worst store    : {scores_df.loc[scores_df['wmae'].idxmax(), 'store_id']} ({scores_df['wmae'].max():.2f})")

if __name__ == "__main__":
    main()