with
    source as (
        select
            CreditCardID,
            CardType
        from {{ source('raw_adventure_works', 'creditcard') }}
    )

    , renamed as (
        select
            cast(CreditCardID as int)  as credit_card_id
            , cast(CardType as string) as card_type
        from source
    )

select *
from renamed