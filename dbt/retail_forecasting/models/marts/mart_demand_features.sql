-- mart_demand_features
-- ML-ready feature table for Phase 4 (Prophet + LightGBM)
-- One row per store+dept+week with all engineered features
-- This is the primary input to the forecasting models

select
    -- identifiers
    store_id,
    dept_id,
    sale_date,

    -- target variable
    weekly_sales,

    -- time features
    extract(year        from sale_date)             as year,
    extract(month       from sale_date)             as month,
    extract(week        from sale_date)             as week_of_year,
    extract(quarter     from sale_date)             as quarter,
    case extract(month from sale_date)
        when 11 then 1
        when 12 then 1
        else 0
    end                                             as is_holiday_season,

    -- holiday flag
    case when is_holiday then 1 else 0 end          as is_holiday,

    -- store features
    store_type,
    store_size,

    -- external features (nulls kept — models handle missing values)
    temperature,
    fuel_price,
    cpi,
    unemployment,

    -- markdown features (binary flags — was there a markdown?)
    case when markdown1 is not null and markdown1 > 0 then 1 else 0 end as has_markdown1,
    case when markdown2 is not null and markdown2 > 0 then 1 else 0 end as has_markdown2,
    case when markdown3 is not null and markdown3 > 0 then 1 else 0 end as has_markdown3,
    case when markdown4 is not null and markdown4 > 0 then 1 else 0 end as has_markdown4,
    case when markdown5 is not null and markdown5 > 0 then 1 else 0 end as has_markdown5,

    -- total markdown value
    coalesce(markdown1, 0) + coalesce(markdown2, 0) + coalesce(markdown3, 0) +
    coalesce(markdown4, 0) + coalesce(markdown5, 0)                     as total_markdown,

    -- lag features
    lag(weekly_sales, 1) over (
        partition by store_id, dept_id order by sale_date
    )                                               as sales_lag_1wk,

    lag(weekly_sales, 4) over (
        partition by store_id, dept_id order by sale_date
    )                                               as sales_lag_4wk,

    lag(weekly_sales, 52) over (
        partition by store_id, dept_id order by sale_date
    )                                               as sales_lag_52wk,

    -- rolling averages
    avg(weekly_sales) over (
        partition by store_id, dept_id
        order by sale_date
        rows between 3 preceding and current row
    )                                               as rolling_4wk_avg,

    avg(weekly_sales) over (
        partition by store_id, dept_id
        order by sale_date
        rows between 12 preceding and current row
    )                                               as rolling_13wk_avg

from {{ ref('stg_walmart_sales') }}