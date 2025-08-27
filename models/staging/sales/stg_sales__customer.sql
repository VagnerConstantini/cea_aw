with
    customer as (
        select
            CustomerID
            , PersonID
            , StoreID
        from {{ source('raw_adventure_works', 'customer') }}
    )

    , person as (
        select
            BusinessEntityID as person_id
            , FirstName
            , MiddleName
            , LastName
        from {{ source('raw_adventure_works', 'person') }}
    )

    , store as (
        select
            BusinessEntityID as store_id
            , Name           as store_name
        from {{ source('raw_adventure_works', 'store') }}
    )

    , final as (
        select
            cast(c.CustomerID as number) as customer_id
            , cast(c.PersonID as number) as person_id
            , cast(c.StoreID  as number) as store_id
            , case
                when s.store_id is not null then 'Store'
                when p.person_id is not null then 'Person'
                else 'Unknown'
            end as customer_type
            , coalesce(
                s.store_name
                , nullif(trim(concat_ws(' ', p.FirstName, p.MiddleName, p.LastName)), '')
            ) as customer_name
        from customer c
        left join person p
            on p.person_id = c.PersonID
        left join store s
            on s.store_id = c.StoreID
    )

select *
from final