-- stg_rossmann_sales
-- Cleans and types the raw Rossmann data
-- Joins train + stores, filters to open stores only

with train as (
    select
        Store,
        DayOfWeek,
        Date,
        Sales,
        Customers,
        Open,
        Promo,
        StateHoliday,
        SchoolHoliday
    from {{ source('retail_raw', 'rossmann_train') }}
    where Open = 1      -- exclude closed store days (Sales = 0, not useful for forecasting)
      and Sales > 0     -- exclude zero-sales days (data quality issue)
),

stores as (
    select
        Store,
        StoreType                                               as store_type,
        Assortment                                              as assortment,
        safe_cast(CompetitionDistance as FLOAT64)               as competition_distance,
        safe_cast(CompetitionOpenSinceMonth as FLOAT64)         as competition_open_since_month,
        safe_cast(CompetitionOpenSinceYear as FLOAT64)          as competition_open_since_year,
        Promo2                                                  as promo2,
        safe_cast(Promo2SinceWeek as FLOAT64)                   as promo2_since_week,
        safe_cast(Promo2SinceYear as FLOAT64)                   as promo2_since_year,
        PromoInterval                                           as promo_interval
    from {{ source('retail_raw', 'rossmann_stores') }}
)

select
    -- keys
    t.Store                                     as store_id,
    t.Date                                      as sale_date,
    t.DayOfWeek                                 as day_of_week,

    -- sales
    t.Sales                                     as daily_sales,
    t.Customers                                 as customers,

    -- promotions
    t.Promo                                     as is_promo,
    t.StateHoliday                              as state_holiday,
    t.SchoolHoliday                             as school_holiday,

    -- store attributes
    s.store_type,
    s.assortment,
    s.competition_distance,
    s.competition_open_since_month,
    s.competition_open_since_year,
    s.promo2,
    s.promo2_since_week,
    s.promo2_since_year,
    s.promo_interval

from train t
left join stores s on t.Store = s.Store