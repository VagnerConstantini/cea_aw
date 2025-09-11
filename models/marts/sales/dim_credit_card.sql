{{ config(materialized='table') }}

with src as (
    select
        credit_card_id
        , card_type
    from {{ ref('stg_sales__credit_card') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['credit_card_id']) }} as credit_card_key
    , cast(credit_card_id as number)                           as credit_card_id
    , cast(card_type      as string)                           as card_type
from src