with
    header as (
        select
              sales_order_id
            , order_date
            , status_code
            , customer_id
            , ship_to_address_id
            , credit_card_id
        from {{ ref('stg_sales__order_header') }}
    )

    , detail as (
        select
              sales_order_id
            , sales_order_detail_id
            , product_id
            , order_qty
            , unit_price
            , unit_price_discount
        from {{ ref('stg_sales__order_detail') }}
    )

    , joined as (
        select
            d.sales_order_id
            , d.sales_order_detail_id
            , d.product_id
            , h.order_date
            , h.status_code
            , h.customer_id
            , h.ship_to_address_id
            , h.credit_card_id
            , d.order_qty
            , d.unit_price
            , d.unit_price_discount
            , cast(d.order_qty * d.unit_price                          as number(18,4)) as gross_amount
            , cast(d.order_qty * d.unit_price * d.unit_price_discount  as number(18,4)) as discount_amount
            , cast(
                (d.order_qty * d.unit_price)
                - (d.order_qty * d.unit_price * d.unit_price_discount) as number(18,4)) as net_amount
        from detail d
        inner join header h
            on h.sales_order_id = d.sales_order_id
    )

select
    sales_order_id
    , sales_order_detail_id
    , product_id
    , order_date
    , status_code
    , customer_id
    , ship_to_address_id
    , credit_card_id
    , order_qty
    , unit_price
    , unit_price_discount
    , gross_amount
    , discount_amount
    , net_amount
from joined