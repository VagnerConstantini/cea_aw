{{ config(materialized='table') }}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day"
        , start_date="cast('2000-01-01' as date)"
        , end_date="cast('2030-12-31' as date)"
    ) }}

)

select
    cast(to_char(date_day, 'yyyymmdd') as number) as date_key
    , date_day
    , extract(year from date_day)                 as year
    , extract(month from date_day)                as month
    , to_char(date_day, 'Month')                  as month_name
    , to_char(date_day, 'yyyy-mm')                as year_month
from spine