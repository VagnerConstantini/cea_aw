{{ config(materialized='table') }}

with src as (
    select
        sales_order_id    as order_id
        , sales_reason_id as sales_reason_id
    from {{ ref('stg_sales__order_sales_reason') }}
)

, joined as (
    select
        s.order_id
        , d.sales_reason_key
    from src s
    inner join {{ ref('dim_sales_reason') }} d
        on d.sales_reason_id = s.sales_reason_id
)

select distinct
    cast(order_id as number) as order_id
    , sales_reason_key
from joined