-- mart_weekly_sales
-- Aggregated weekly sales at store+dept level with enriched features
-- Primary table for demand forecasting models

select
    store_id,
    dept_id,
    sale_date,
    weekly_sales,
    is_holiday,
    store_type,
    store_size,
    temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    cpi,
    unemployment,

    -- derived time features
    extract(year  from sale_date)                   as year,
    extract(month from sale_date)                   as month,
    extract(week  from sale_date)                   as week_of_year,

    -- rolling 4-week average sales per store+dept
    avg(weekly_sales) over (
        partition by store_id, dept_id
        order by sale_date
        rows between 3 preceding and current row
    )                                               as rolling_4wk_avg_sales,

    -- year-over-year flag (52 weeks back)
    lag(weekly_sales, 52) over (
        partition by store_id, dept_id
        order by sale_date
    )                                               as sales_same_week_last_year

from {{ ref('stg_walmart_sales') }}