"""
Retail Demand Forecasting API
------------------------------
FastAPI app deployed on Cloud Run.
Reads forecast and risk data from BigQuery.

Endpoints:
  GET /health                        → health check
  GET /stores                        → list all store IDs
  GET /forecast?store_id=1&model=lightgbm  → latest forecast for a store
  GET /risk?store_id=1               → inventory risk tier for a store
"""

import os
from fastapi import FastAPI, HTTPException, Query
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "retail-pipeline-poc")
BQ_CLIENT  = None  # lazy init

app = FastAPI(
    title="Retail Demand Forecasting API",
    description="Serves demand forecasts and inventory risk scores from BigQuery",
    version="1.0.0"
)


def get_client():
    global BQ_CLIENT
    if BQ_CLIENT is None:
        BQ_CLIENT = bigquery.Client(project=PROJECT_ID)
    return BQ_CLIENT


@app.get("/health")
def health():
    return {"status": "ok", "project": PROJECT_ID}


@app.get("/stores")
def list_stores():
    """Returns all store IDs available in the forecast table."""
    query = f"""
        SELECT DISTINCT store_id
        FROM `{PROJECT_ID}.retail_marts.mart_demand_forecast`
        ORDER BY store_id
    """
    try:
        rows = get_client().query(query).result()
        stores = [row.store_id for row in rows]
        return {"store_count": len(stores), "store_ids": stores}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/forecast")
def get_forecast(
    store_id: int = Query(..., description="Store ID (1-45)"),
    model: str    = Query("lightgbm", description="Model: lightgbm or prophet"),
    weeks: int    = Query(12, description="Number of forecast weeks to return", ge=1, le=52)
):
    """Returns the latest demand forecast for a given store."""
    query = f"""
        SELECT
            store_id,
            dept_id,
            ds          as forecast_date,
            yhat        as forecast_sales,
            yhat_lower  as forecast_lower,
            yhat_upper  as forecast_upper,
            model,
            run_id,
            run_ts
        FROM `{PROJECT_ID}.retail_marts.mart_demand_forecast`
        WHERE store_id = {store_id}
          AND LOWER(model) = LOWER('{model}')
          AND run_ts = (
              SELECT MAX(run_ts)
              FROM `{PROJECT_ID}.retail_marts.mart_demand_forecast`
              WHERE store_id = {store_id} AND LOWER(model) = LOWER('{model}')
          )
        ORDER BY forecast_date
        LIMIT {weeks * 100}
    """
    try:
        rows = list(get_client().query(query).result())
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No forecast found for store_id={store_id} model={model}"
            )
        return {
            "store_id"  : store_id,
            "model"     : model,
            "run_id"    : rows[0].run_id,
            "forecasts" : [
                {
                    "date"    : str(r.forecast_date),
                    "dept_id" : r.dept_id,
                    "yhat"    : round(r.forecast_sales, 2),
                    "lower"   : round(r.forecast_lower, 2),
                    "upper"   : round(r.forecast_upper, 2),
                }
                for r in rows
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/risk")
def get_risk(
    store_id: int = Query(..., description="Store ID (1-45)")
):
    """Returns inventory risk tiers for all departments in a store."""
    query = f"""
        SELECT
            store_id,
            dept_id,
            store_type,
            avg_weekly_sales,
            stddev_weekly_sales,
            coeff_of_variation,
            demand_risk_tier
        FROM `{PROJECT_ID}.retail_marts.mart_inventory_risk`
        WHERE store_id = {store_id}
        ORDER BY coeff_of_variation DESC
    """
    try:
        rows = list(get_client().query(query).result())
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No risk data found for store_id={store_id}"
            )

        high   = sum(1 for r in rows if r.demand_risk_tier == "HIGH")
        medium = sum(1 for r in rows if r.demand_risk_tier == "MEDIUM")
        low    = sum(1 for r in rows if r.demand_risk_tier == "LOW")

        return {
            "store_id"  : store_id,
            "store_type": rows[0].store_type,
            "summary"   : {"HIGH": high, "MEDIUM": medium, "LOW": low},
            "departments": [
                {
                    "dept_id"           : r.dept_id,
                    "avg_weekly_sales"  : round(r.avg_weekly_sales, 2),
                    "coeff_of_variation": round(r.coeff_of_variation, 4),
                    "risk_tier"         : r.demand_risk_tier,
                }
                for r in rows
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))