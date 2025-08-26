with
    source as (
        select
            SalesOrderID
            , OrderDate
            , Status
            , CustomerID
            , ShipToAddressID
            , CreditCardID
            , SubTotal
            , TaxAmt
            , Freight
            , TotalDue
        from {{ source('raw_adventure_works', 'salesorderheader') }}
    )

    , renamed as (
        select
            cast(SalesOrderID as number)      as sales_order_id
            , cast(OrderDate as date)         as order_date
            , cast(Status as number)          as status_code
            , cast(CustomerID as number)      as customer_id
            , cast(ShipToAddressID as number) as ship_to_address_id
            , cast(CreditCardID as number)    as credit_card_id
            , cast(SubTotal as number(18,2))  as subtotal_amount
            , cast(TaxAmt as number(18,2))    as tax_amount
            , cast(Freight as number(18,2))   as freight_amount
            , cast(TotalDue as number(18,2))  as total_due_amount
        from source
    )

select *
from renamed