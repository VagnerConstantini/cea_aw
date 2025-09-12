{{ config(materialized='table') }}

with src as (
    select
        sales_reason_id
        , reason_name
        , reason_type
    from {{ ref('stg_sales__sales_reason') }}
)

select
    {{ dbt_utils.generate_surrogate_key(["'SR0'", "sales_reason_id"]) }} as sales_reason_key
    , cast(sales_reason_id as number)                                    as sales_reason_id
    , cast(reason_name     as string)                                    as reason_name
    , cast(reason_type     as string)                                    as reason_type
from src