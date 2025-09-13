{{ config(materialized='table') }}

with src as (
    select
        cast(order_status_code   as number) as status_code
        , cast(order_status_name as string) as status_name
    from {{ ref('order_status') }}  -- seed
)

select
    {{ dbt_utils.generate_surrogate_key(["'OS0'", "status_code"]) }} as order_status_key
    , status_code
    , status_name
from src