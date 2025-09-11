{{ config(materialized='table') }}

with
    src as (
        select
            customer_id
            , customer_type
            , customer_name
            , person_id
            , store_id
        from {{ ref('stg_sales__customer') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key
    , cast(customer_id   as number)                         as customer_id
    , cast(customer_type as string)                         as customer_type
    , cast(customer_name as string)                         as customer_name
    , cast(person_id     as number)                         as person_id
    , cast(store_id      as number)                         as store_id
from src