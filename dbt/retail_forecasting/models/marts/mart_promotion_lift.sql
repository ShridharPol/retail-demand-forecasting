-- mart_promotion_lift
-- Measures promotional lift using Rossmann data
-- Compares promo vs non-promo sales per store
-- Key business insight: which stores benefit most from promotions

select
    store_id,
    store_type,
    assortment,
    competition_distance,

    -- promo sales
    round(avg(case when is_promo = 1 then daily_sales end), 2)      as avg_promo_sales,
    round(avg(case when is_promo = 0 then daily_sales end), 2)      as avg_non_promo_sales,

    -- lift = (promo - non_promo) / non_promo * 100
    round(
        safe_divide(
            avg(case when is_promo = 1 then daily_sales end) -
            avg(case when is_promo = 0 then daily_sales end),
            avg(case when is_promo = 0 then daily_sales end)
        ) * 100,
    2)                                                               as promo_lift_pct,

    -- volume
    count(case when is_promo = 1 then 1 end)                        as promo_days,
    count(case when is_promo = 0 then 1 end)                        as non_promo_days,
    round(sum(daily_sales), 2)                                      as total_sales

from {{ ref('stg_rossmann_sales') }}
group by store_id, store_type, assortment, competition_distance