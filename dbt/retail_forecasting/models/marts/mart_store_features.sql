-- mart_store_features
-- One row per store with aggregated performance metrics
-- Used for store-level segmentation and feature engineering

select
    store_id,
    store_type,
    store_size,

    -- sales aggregates
    round(avg(weekly_sales), 2)                     as avg_weekly_sales,
    round(sum(weekly_sales), 2)                     as total_sales,
    round(min(weekly_sales), 2)                     as min_weekly_sales,
    round(max(weekly_sales), 2)                     as max_weekly_sales,
    round(stddev(weekly_sales), 2)                  as stddev_weekly_sales,

    -- holiday impact
    round(avg(case when is_holiday then weekly_sales end), 2)     as avg_holiday_sales,
    round(avg(case when not is_holiday then weekly_sales end), 2) as avg_non_holiday_sales,

    -- count of weeks
    count(distinct sale_date)                       as num_weeks,
    count(distinct dept_id)                         as num_depts,

    -- external features (store-level averages)
    round(avg(temperature), 2)                      as avg_temperature,
    round(avg(fuel_price), 2)                       as avg_fuel_price,
    round(avg(cpi), 2)                              as avg_cpi,
    round(avg(unemployment), 2)                     as avg_unemployment

from {{ ref('stg_walmart_sales') }}
group by store_id, store_type, store_size