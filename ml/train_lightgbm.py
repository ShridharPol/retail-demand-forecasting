"""
LightGBM Global Forecasting Model
-----------------------------------
- Reads mart_demand_features from BigQuery
- Trains a single global LightGBM model across all stores and depts
- Uses engineered lag + rolling features from the mart
- Logs parameters, metrics, feature importance to MLflow
- Writes forecasts to a local parquet file for forecast_output.py
- Metric: WMAE (Weighted Mean Absolute Error)
"""

import os
import logging
import warnings
import pandas as pd
import numpy as np
import mlflow
import mlflow.lightgbm
import lightgbm as lgb
from sklearn.model_selection import TimeSeriesSplit
from google.cloud import bigquery

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
PROJECT_ID       = os.environ.get("GCP_PROJECT_ID", "retail-pipeline-poc")
BQ_DATASET       = "retail_marts"
TABLE            = "mart_demand_features"
MLFLOW_URI       = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
FORECAST_HORIZON = 12
OUTPUT_PATH      = "ml/lgbm_forecasts.parquet"

# ── MLflow setup ─────────────────────────────────────────────
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("lightgbm_global")

# ── Feature columns used for training ────────────────────────
FEATURE_COLS = [
    "store_id", "dept_id",
    "year", "month", "week_of_year", "quarter",
    "is_holiday", "is_holiday_season",
    "store_size",
    "has_markdown1", "has_markdown2", "has_markdown3",
    "has_markdown4", "has_markdown5", "total_markdown",
    "sales_lag_1wk", "sales_lag_4wk", "sales_lag_52wk",
    "rolling_4wk_avg", "rolling_13wk_avg",
]

TARGET_COL = "weekly_sales"

# ── Load data ────────────────────────────────────────────────
def load_data():
    client = bigquery.Client(project=PROJECT_ID)
    query  = f"""
        SELECT *
        FROM `{PROJECT_ID}.{BQ_DATASET}.{TABLE}`
        WHERE weekly_sales > 0
        ORDER BY store_id, dept_id, sale_date
    """
    log.info("Loading mart_demand_features from BigQuery...")
    df = client.query(query).to_dataframe()
    log.info(f"Loaded {len(df):,} rows")
    return df

# ── WMAE ─────────────────────────────────────────────────────
def wmae(actual, predicted, weights):
    return np.sum(weights * np.abs(actual - predicted)) / np.sum(weights)

# ── Encode store_type ─────────────────────────────────────────
def encode_features(df):
    # encode store_type as integer
    if "store_type" in df.columns:
        df["store_type"] = df["store_type"].map({"A": 0, "B": 1, "C": 2}).fillna(-1)
    return df

# ── Main ─────────────────────────────────────────────────────
def main():
    df = load_data()
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df = encode_features(df)

    # Drop rows with nulls in lag features (first few weeks won't have lags)
    df = df.dropna(subset=["sales_lag_1wk", "sales_lag_4wk", "rolling_4wk_avg"])
    log.info(f"After dropping lag nulls: {len(df):,} rows")

    # Time-based train/test split — last 12 weeks as holdout
    cutoff    = df["sale_date"].max() - pd.Timedelta(weeks=FORECAST_HORIZON)
    train_df  = df[df["sale_date"] <= cutoff]
    test_df   = df[df["sale_date"] >  cutoff]

    log.info(f"Train: {len(train_df):,} rows | Test: {len(test_df):,} rows")

    # Use only columns that exist in the dataframe
    available_features = [c for c in FEATURE_COLS if c in df.columns]

    X_train = train_df[available_features]
    y_train = train_df[TARGET_COL]
    X_test  = test_df[available_features]
    y_test  = test_df[TARGET_COL]
    w_test  = np.where(test_df["is_holiday"] == 1, 5, 1)

    # LightGBM parameters
    params = {
        "objective"       : "regression_l1",  # MAE objective
        "metric"          : "mae",
        "learning_rate"   : 0.05,
        "num_leaves"      : 64,
        "min_child_samples": 20,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq"    : 5,
        "verbose"         : -1,
        "n_estimators"    : 500,
    }

    with mlflow.start_run(run_name="lgbm_global"):
        mlflow.log_params(params)
        mlflow.log_param("train_rows",        len(train_df))
        mlflow.log_param("test_rows",         len(test_df))
        mlflow.log_param("num_features",      len(available_features))
        mlflow.log_param("forecast_horizon",  FORECAST_HORIZON)

        model = lgb.LGBMRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(100)]
        )

        # Evaluate
        preds  = model.predict(X_test)
        preds  = np.maximum(preds, 0)  # no negative sales
        score  = wmae(y_test.values, preds, w_test)

        mlflow.log_metric("wmae", round(score, 2))
        mlflow.log_metric("best_iteration", model.best_iteration_)
        mlflow.lightgbm.log_model(model, "lgbm_model")

        log.info(f"✅ LightGBM WMAE: {score:.2f}")

        # Feature importance
        fi = pd.DataFrame({
            "feature"   : available_features,
            "importance": model.feature_importances_
        }).sort_values("importance", ascending=False)

        log.info(f"\n📊 Top 10 features:\n{fi.head(10).to_string(index=False)}")

        fi_path = "ml/feature_importance.csv"
        fi.to_csv(fi_path, index=False)
        mlflow.log_artifact(fi_path)

        # Build forecast output
        test_df = test_df.copy()
        test_df["yhat"]    = preds
        test_df["model"]   = "lightgbm"
        test_df["ds"]      = test_df["sale_date"]
        test_df["yhat_lower"] = preds * 0.9   # simple 10% confidence interval
        test_df["yhat_upper"] = preds * 1.1

        out = test_df[["store_id", "dept_id", "ds", "yhat", "yhat_lower", "yhat_upper", "model"]]
        out.to_parquet(OUTPUT_PATH, index=False)
        log.info(f"✅ Saved LightGBM forecasts → {OUTPUT_PATH}")

if __name__ == "__main__":
    main()