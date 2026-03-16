-- mart_inventory_risk
-- Identifies store+dept combinations with high sales volatility
-- High CV (coefficient of variation) = unpredictable demand = inventory risk
-- Business use: prioritize forecasting effort on high-risk store/dept combos

with sales_stats as (
    select
        store_id,
        dept_id,
        store_type,
        round(avg(weekly_sales), 2)                 as avg_weekly_sales,
        round(stddev(weekly_sales), 2)              as stddev_weekly_sales,
        round(min(weekly_sales), 2)                 as min_weekly_sales,
        round(max(weekly_sales), 2)                 as max_weekly_sales,
        count(*)                                    as num_weeks,
        round(sum(weekly_sales), 2)                 as total_sales
    from {{ ref('stg_walmart_sales') }}
    group by store_id, dept_id, store_type
),

with_cv as (
    select
        *,
        -- coefficient of variation = stddev / mean (higher = more volatile)
        round(safe_divide(stddev_weekly_sales, avg_weekly_sales), 4) as coeff_of_variation,

        -- risk tier
        case
            when safe_divide(stddev_weekly_sales, avg_weekly_sales) > 0.5 then 'HIGH'
            when safe_divide(stddev_weekly_sales, avg_weekly_sales) > 0.25 then 'MEDIUM'
            else 'LOW'
        end                                         as demand_risk_tier

    from sales_stats
    where avg_weekly_sales > 0
)

select * from with_cv
order by coeff_of_variation desc