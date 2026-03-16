-- stg_online_retail
-- Cleans the Online Retail II transactional data
-- Handles: cancellations, nulls, negative quantities, type casting

with raw as (
    select
        Invoice,
        StockCode,
        Description,
        Quantity,
        InvoiceDate,
        Price,
        Customer_ID,
        Country
    from {{ source('retail_raw', 'online_retail_raw') }}
),

cleaned as (
    select
        Invoice                                                     as invoice_id,
        StockCode                                                   as stock_code,
        trim(Description)                                           as description,
        cast(Quantity as INT64)                                     as quantity,

        -- InvoiceDate comes as string e.g. "2010-12-01 08:26:00"
        parse_timestamp('%Y-%m-%d %H:%M:%S', InvoiceDate)          as invoice_timestamp,
        date(parse_timestamp('%Y-%m-%d %H:%M:%S', InvoiceDate))    as invoice_date,

        cast(Price as FLOAT64)                                      as unit_price,
        Customer_ID                                                 as customer_id,
        Country                                                     as country,

        -- derived fields
        cast(Quantity as INT64) * cast(Price as FLOAT64)            as revenue,

        -- flag cancelled orders (Invoice starts with 'C')
        starts_with(Invoice, 'C')                                   as is_cancellation

    from raw
    where
        StockCode is not null
        and Description is not null
        and Quantity is not null
        and Price is not null
        and cast(Price as FLOAT64) > 0      -- remove zero-price items
)

select *
from cleaned