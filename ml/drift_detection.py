"""
Drift Detection
---------------
Compares the distribution of recent data (last 12 weeks) against
the training baseline using Population Stability Index (PSI).

PSI thresholds (industry standard):
  < 0.1  → No significant drift   (LOW)
  0.1–0.2 → Moderate drift        (MEDIUM) — monitor closely
  > 0.2  → Significant drift      (HIGH)   — consider retraining

Reads from:  BigQuery retail_marts.mart_demand_features
Logs to:     MLflow experiment 'drift_detection'
Writes to:   ml/drift_report.json
"""

import os
import json
import logging
import warnings
import numpy as np
import pandas as pd
import mlflow
from google.cloud import bigquery
from datetime import datetime

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
PROJECT_ID       = os.environ.get("GCP_PROJECT_ID", "retail-pipeline-poc")
BQ_DATASET       = "retail_marts"
TABLE            = "mart_demand_features"
MLFLOW_URI       = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
RECENT_WEEKS     = 12
OUTPUT_PATH      = "ml/drift_report.json"

# Same features used in train_lightgbm.py
NUMERIC_FEATURES = [
    "store_size",
    "total_markdown",
    "sales_lag_1wk", "sales_lag_4wk", "sales_lag_52wk",
    "rolling_4wk_avg", "rolling_13wk_avg",
    "weekly_sales",
]

CATEGORICAL_FEATURES = [
    "store_id", "dept_id",
    "week_of_year", "month", "quarter",
    "is_holiday", "is_holiday_season",
    "has_markdown1", "has_markdown2", "has_markdown3",
    "has_markdown4", "has_markdown5",
]

PSI_LOW    = 0.1
PSI_HIGH   = 0.2

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("drift_detection")


# ── Load data ─────────────────────────────────────────────────
def load_data():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{BQ_DATASET}.{TABLE}`
        WHERE weekly_sales > 0
        ORDER BY sale_date
    """
    log.info("Loading mart_demand_features from BigQuery...")
    df = client.query(query).to_dataframe()
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    log.info(f"Loaded {len(df):,} rows | Date range: {df['sale_date'].min().date()} → {df['sale_date'].max().date()}")
    return df


# ── PSI calculation ───────────────────────────────────────────
def psi(baseline: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index between two distributions."""
    # Build bin edges from baseline
    breakpoints = np.percentile(baseline, np.linspace(0, 100, bins + 1))
    breakpoints = np.unique(breakpoints)  # handle duplicate edges
    if len(breakpoints) < 3:
        return 0.0  # not enough variation to compute PSI

    baseline_counts, _ = np.histogram(baseline, bins=breakpoints)
    current_counts,  _ = np.histogram(current,  bins=breakpoints)

    # Convert to proportions, avoid division by zero
    baseline_pct = np.where(baseline_counts == 0, 1e-6, baseline_counts / len(baseline))
    current_pct  = np.where(current_counts  == 0, 1e-6, current_counts  / len(current))

    psi_val = np.sum((current_pct - baseline_pct) * np.log(current_pct / baseline_pct))
    return round(float(psi_val), 4)


def psi_categorical(baseline: pd.Series, current: pd.Series) -> float:
    """PSI for categorical features using value counts."""
    all_cats   = set(baseline.unique()) | set(current.unique())
    base_pct   = baseline.value_counts(normalize=True).reindex(all_cats, fill_value=1e-6)
    curr_pct   = current.value_counts(normalize=True).reindex(all_cats, fill_value=1e-6)
    psi_val    = np.sum((curr_pct - base_pct) * np.log(curr_pct / base_pct))
    return round(float(psi_val), 4)


def drift_tier(psi_val: float) -> str:
    if psi_val < PSI_LOW:
        return "LOW"
    elif psi_val < PSI_HIGH:
        return "MEDIUM"
    return "HIGH"


# ── Main ──────────────────────────────────────────────────────
def main():
    df = load_data()

    cutoff     = df["sale_date"].max() - pd.Timedelta(weeks=RECENT_WEEKS)
    baseline   = df[df["sale_date"] <= cutoff]
    recent     = df[df["sale_date"] >  cutoff]

    log.info(f"Baseline: {len(baseline):,} rows (up to {cutoff.date()})")
    log.info(f"Recent  : {len(recent):,} rows (last {RECENT_WEEKS} weeks)")

    if len(recent) == 0:
        log.error("No recent data found — cannot compute drift.")
        return

    results  = {}
    n_high   = 0
    n_medium = 0

    with mlflow.start_run(run_name=f"drift_{datetime.utcnow().strftime('%Y%m%d_%H%M')}"):
        mlflow.log_param("baseline_rows",  len(baseline))
        mlflow.log_param("recent_rows",    len(recent))
        mlflow.log_param("recent_weeks",   RECENT_WEEKS)
        mlflow.log_param("cutoff_date",    str(cutoff.date()))

        # Numeric features
        log.info("\n── Numeric Feature Drift ──────────────────────────")
        for feat in NUMERIC_FEATURES:
            if feat not in df.columns:
                continue
            b = baseline[feat].dropna().values
            r = recent[feat].dropna().values
            if len(b) == 0 or len(r) == 0:
                continue

            psi_val = psi(b, r)
            tier    = drift_tier(psi_val)
            results[feat] = {"psi": psi_val, "tier": tier, "type": "numeric"}

            mlflow.log_metric(f"psi_{feat}", psi_val)

            symbol = "✅" if tier == "LOW" else ("⚠️ " if tier == "MEDIUM" else "🚨")
            log.info(f"  {symbol} {feat:<25} PSI={psi_val:.4f}  [{tier}]")

            if tier == "HIGH":   n_high   += 1
            if tier == "MEDIUM": n_medium += 1

        # Categorical features
        log.info("\n── Categorical Feature Drift ──────────────────────")
        for feat in CATEGORICAL_FEATURES:
            if feat not in df.columns:
                continue

            psi_val = psi_categorical(baseline[feat], recent[feat])
            tier    = drift_tier(psi_val)
            results[feat] = {"psi": psi_val, "tier": tier, "type": "categorical"}

            mlflow.log_metric(f"psi_{feat}", psi_val)

            symbol = "✅" if tier == "LOW" else ("⚠️ " if tier == "MEDIUM" else "🚨")
            log.info(f"  {symbol} {feat:<25} PSI={psi_val:.4f}  [{tier}]")

            if tier == "HIGH":   n_high   += 1
            if tier == "MEDIUM": n_medium += 1

        # Summary
        mlflow.log_metric("n_high_drift_features",   n_high)
        mlflow.log_metric("n_medium_drift_features", n_medium)

        log.info(f"\n── Summary ────────────────────────────────────────")
        log.info(f"  🚨 HIGH drift features   : {n_high}")
        log.info(f"  ⚠️  MEDIUM drift features : {n_medium}")
        log.info(f"  ✅ LOW drift features    : {len(results) - n_high - n_medium}")

        if n_high > 0:
            high_feats = [f for f, v in results.items() if v["tier"] == "HIGH"]
            log.warning(f"\n  ⚠️  DRIFT DETECTED in: {high_feats}")
            log.warning(f"  Consider retraining the LightGBM model.")
        else:
            log.info(f"\n  ✅ No significant drift detected. Model is stable.")

        # Write report
        report = {
            "run_timestamp" : datetime.utcnow().isoformat(),
            "cutoff_date"   : str(cutoff.date()),
            "recent_weeks"  : RECENT_WEEKS,
            "baseline_rows" : len(baseline),
            "recent_rows"   : len(recent),
            "summary"       : {"high": n_high, "medium": n_medium,
                               "low": len(results) - n_high - n_medium},
            "features"      : results,
        }

        with open(OUTPUT_PATH, "w") as f:
            json.dump(report, f, indent=2)

        mlflow.log_artifact(OUTPUT_PATH)
        log.info(f"\n✅ Drift report saved → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()