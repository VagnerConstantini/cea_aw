{{ config(materialized='table') }}

with src as (
    select
        credit_card_id
        , card_type
    from {{ ref('stg_sales__credit_card') }}
)

unknown as (
    -- linha técnica para casos sem cartão
    select
        -1          as credit_card_id,
        , 'Unknown' as card_type
),

unioned as (
    select * from src
    union all
    select * from unknown
)   

select
    {{ dbt_utils.generate_surrogate_key(["'CC0'", "credit_card_id"]) }} as credit_card_key
    , cast(credit_card_id as number) as credit_card_id
    , cast(card_type      as string) as card_type
from unioned