{{ config(materialized='table') }}

with
    src as (
        select
            product_id
            , product_name
            , subcategory_name
            , category_name
        from {{ ref('stg_production__product') }}
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key
            , cast(product_id as number(38,0))                     as product_id
            , cast(product_name as string)                         as product_name
            , cast(subcategory_name as string)                     as product_subcategory_name
            , cast(category_name as string)                        as product_category_name
        from src
    )

select *
from final