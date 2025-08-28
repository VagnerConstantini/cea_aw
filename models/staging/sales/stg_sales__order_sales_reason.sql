with
    source as (
        select
            SalesOrderID
            , SalesReasonID
        from {{ source('raw_adventure_works', 'salesorderheadersalesreason') }}
    )

    , renamed as (
        select
            cast(SalesOrderID as number)    as sales_order_id
            , cast(SalesReasonID as number) as sales_reason_id
        from source
    )

select *
from renamed