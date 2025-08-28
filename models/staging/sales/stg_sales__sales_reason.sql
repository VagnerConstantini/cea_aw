with
    source as (
        select
            SalesReasonID
            , Name
            , ReasonType
        from {{ source('raw_adventure_works', 'salesreason') }}
    )

    , renamed as (
        select
            cast(SalesReasonID as number)  as sales_reason_id
            , nullif(trim(Name), '')       as reason_name
            , nullif(trim(ReasonType), '') as reason_type
        from source
    )

select *
from renamed