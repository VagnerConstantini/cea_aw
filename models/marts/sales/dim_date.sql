{{ config(materialized='table') }}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day"
        , start_date="cast('2000-01-01' as date)"
        , end_date="cast('2030-12-31' as date)"
    ) }}

)

select
    {{ dbt_utils.generate_surrogate_key(["'DT0'", "date_day"]) }} as date_key
    , cast(date_day                     as date)                  as date_day
    , cast(extract(year  from date_day) as number)                as year
    , cast(extract(month from date_day) as number)                as month_number
    , cast(to_char(date_day, 'Mon')     as string)                as month_name   -- "Jan", "Feb", ...
    , cast(to_char(date_day, 'YYYY-MM') as string)                as year_month   -- "2025-09"
from spine