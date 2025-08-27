with
    product as (
        select
            productid              as product_id
            , name                 as product_name
            , productsubcategoryid as product_subcategory_id
        from {{ source('raw_adventure_works', 'product') }}
    )

    , subcategory as (
        select
            productsubcategoryid as product_subcategory_id
            , name               as subcategory_name
            , productcategoryid  as product_category_id
        from {{ source('raw_adventure_works', 'productsubcategory') }}
    )

    , category as (
        select
            productcategoryid as product_category_id
            , name            as category_name
        from {{ source('raw_adventure_works', 'productcategory') }}
    )

    , joined as (
        select
            p.product_id
            , p.product_name
            , s.subcategory_name
            , c.category_name
        from product p
        left join subcategory s
            on s.product_subcategory_id = p.product_subcategory_id
        left join category c
            on c.product_category_id = s.product_category_id
    )

select *
from joined