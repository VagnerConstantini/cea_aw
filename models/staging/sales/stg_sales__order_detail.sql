with
    source as (
        select
            SalesOrderID
            , SalesOrderDetailID
            , ProductID
            , OrderQty
            , UnitPrice
            , UnitPriceDiscount
        from {{ source('raw_adventure_works', 'salesorderdetail') }}
    )

    , renamed as (
        select
            cast(SalesOrderID as number)             as sales_order_id
            , cast(SalesOrderDetailID as number)     as sales_order_detail_id
            , cast(ProductID as number)              as product_id
            , cast(OrderQty as number)               as order_qty
            , cast(UnitPrice as number(18,4))        as unit_price
            , cast(UnitPriceDiscount as number(9,4)) as unit_price_discount
        from source
    )

select *
from renamed