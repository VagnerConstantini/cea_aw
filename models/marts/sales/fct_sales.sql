{{ config(materialized='table') }}

with lines as (
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
    from {{ ref('int_sales__order_line') }}
)

, geo as (
    select
        address_id
        , city_name
        , state_province_code
        , country_region_code
    from {{ ref('int_geography__ship_to') }}
)

, joined as (
    select
        l.sales_order_id
        , l.sales_order_detail_id
        , l.product_id
        , l.order_date
        , l.status_code
        , l.customer_id
        , l.ship_to_address_id
        , l.credit_card_id
        , l.order_qty
        , l.unit_price
        , l.unit_price_discount
        , l.gross_amount
        , l.discount_amount
        , l.net_amount
        , g.city_name
        , g.state_province_code
        , g.country_region_code
    from lines l
    left join geo g
        on g.address_id = l.ship_to_address_id
)

, dim_keys as (
    select
        j.sales_order_id
        , j.sales_order_detail_id
        , j.status_code
        , j.customer_id
        , j.credit_card_id
        , j.product_id
        , j.order_qty
        , j.unit_price
        , j.unit_price_discount
        , j.gross_amount
        , j.discount_amount
        , j.net_amount
        , dpr.product_key
        , dcu.customer_key
        , dcc.credit_card_key
        , dge.geography_key
        , dde.date_key as order_date_key
        , dos.order_status_key
    from joined j
    -- product
    left join {{ ref('dim_product') }} dpr
        on dpr.product_id = j.product_id
    -- customer
    left join {{ ref('dim_customer') }} dcu
        on dcu.customer_id = j.customer_id
    -- credit card (nullable)
    left join {{ ref('dim_credit_card') }} dcc
        on dcc.credit_card_id = j.credit_card_id
    -- geography
    left join {{ ref('dim_geography') }} dge
        on  dge.country_code        = j.country_region_code
        and dge.state_province_code = j.state_province_code
        and lower(dge.city)         = lower(j.city_name)
    -- date
    left join {{ ref('dim_date') }} dde
        on dde.date_day = j.order_date
    -- order status (seed-based dim)
    left join {{ ref('dim_order_status') }} dos
        on dos.status_code = j.status_code
)

select
    {{ dbt_utils.generate_surrogate_key(
        ["'FS0'", "sales_order_id", "sales_order_detail_id"]
    ) }}                                          as sales_order_line_key
    , cast(sales_order_id        as number)       as sales_order_id
    , cast(sales_order_detail_id as number)       as sales_order_detail_id
    , product_key
    , customer_key
    , credit_card_key
    , geography_key
    , order_date_key
    , order_status_key
    , cast(status_code           as number)       as status_code
    , cast(order_qty             as number(18,0)) as order_qty
    , cast(unit_price            as number(18,2)) as unit_price
    , cast(unit_price_discount   as number(9,4))  as unit_price_discount
    , cast(gross_amount          as number(18,2)) as gross_amount
    , cast(discount_amount       as number(18,2)) as discount_amount
    , cast(net_amount            as number(18,2)) as net_amount
from dim_keys