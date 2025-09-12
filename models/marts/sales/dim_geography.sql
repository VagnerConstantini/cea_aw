{{ config(materialized='table') }}

with src as (
    select
        city_name
        , state_province_name
        , state_province_code
        , country_region_name
        , country_region_code
    from {{ ref('int_geography__ship_to') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        "'GE0'"
        , "country_region_code"
        , "state_province_code"
        , "lower(city_name)"
    ]) }}                                 as geography_key
    , cast(city_name           as string) as city
    , cast(state_province_name as string) as state_province
    , cast(state_province_code as string) as state_province_code
    , cast(country_region_name as string) as country
    , cast(country_region_code as string) as country_code
from src
group by
    city_name
    , state_province_name
    , state_province_code
    , country_region_name
    , country_region_code