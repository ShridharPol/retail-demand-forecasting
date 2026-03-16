-- stg_walmart_sales
-- Cleans and types the raw Walmart data
-- Joins train + stores + features into one staging table
-- NA strings in features are cast to NULL

with train as (
    select
        Store,
        Dept,
        Date,
        Weekly_Sales,
        IsHoliday
    from {{ source('retail_raw', 'walmart_train') }}
),

stores as (
    select
        Store,
        Type    as store_type,
        Size    as store_size
    from {{ source('retail_raw', 'walmart_stores') }}
),

features as (
    select
        cast(Store       as INTEGER)  as Store,
        parse_date('%Y-%m-%d', Date)  as feature_date,
        safe_cast(Temperature  as FLOAT64)                          as temperature,
        safe_cast(Fuel_Price   as FLOAT64)                          as fuel_price,
        safe_cast(nullif(MarkDown1, 'NA') as FLOAT64)               as markdown1,
        safe_cast(nullif(MarkDown2, 'NA') as FLOAT64)               as markdown2,
        safe_cast(nullif(MarkDown3, 'NA') as FLOAT64)               as markdown3,
        safe_cast(nullif(MarkDown4, 'NA') as FLOAT64)               as markdown4,
        safe_cast(nullif(MarkDown5, 'NA') as FLOAT64)               as markdown5,
        safe_cast(nullif(CPI,          'NA') as FLOAT64)            as cpi,
        safe_cast(nullif(Unemployment, 'NA') as FLOAT64)            as unemployment,
        (IsHoliday = 'TRUE')                                        as is_holiday_feature
    from {{ source('retail_raw', 'walmart_features') }}
)

select
    -- keys
    t.Store                                     as store_id,
    t.Dept                                      as dept_id,
    t.Date                                      as sale_date,

    -- sales
    t.Weekly_Sales                              as weekly_sales,
    t.IsHoliday                                 as is_holiday,

    -- store attributes
    s.store_type,
    s.store_size,

    -- external features (left join — not all weeks have features)
    f.temperature,
    f.fuel_price,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5,
    f.cpi,
    f.unemployment

from train t
left join stores   s on t.Store = s.Store
left join features f on t.Store = f.Store and t.Date = f.feature_date