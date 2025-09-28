{{ config(materialized='table') }}

with bounds as (
    -- Get the minimum and maximum sales dates from the order header
    select
        min(order_date) as min_date,
        max(order_date) as max_date
    from {{ ref('stg_sales__order_header') }}
),

spine as (
    -- Generate one row per day between min_date and max_date
    -- Note: GENERATOR requires a constant ROWCOUNT. We use a safe upper bound
    -- and then filter by max_date.
    select
        dateadd(day, seq4(), b.min_date) as date_day
    from bounds b
    cross join table(generator(rowcount => 50000)) g  -- safe upper bound of days
    where dateadd(day, seq4(), b.min_date) <= b.max_date
)

select
    {{ dbt_utils.generate_surrogate_key(["'DT0'", "date_day"]) }} as date_key,
    cast(date_day                     as date)   as date_day,
    cast(extract(year  from date_day) as number) as year,
    cast(extract(month from date_day) as number) as month_number,
    cast(to_char(date_day, 'Mon')     as string) as month_name,       -- "Jan", "Feb", ...
    cast(to_char(date_day, 'YYYY-MM') as string) as year_month,       -- "2025-09"
    cast(to_char(date_day, 'YYYYMM')  as number) as year_month_sort   -- 202509
from spine