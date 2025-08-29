with
    address as (
        select
            AddressID
            , City
            , StateProvinceID
        from {{ source('raw_adventure_works', 'address') }}
    )

    , stateprovince as (
        select
            StateProvinceID
            , Name as StateProvinceName
            , StateProvinceCode
            , CountryRegionCode
        from {{ source('raw_adventure_works', 'stateprovince') }}
    )

    , country as (
        select
            CountryRegionCode
            , Name as CountryRegionName
        from {{ source('raw_adventure_works', 'countryregion') }}
    )

    , joined as (
        select
            cast(a.AddressID as number(38,0))                       as address_id
            , cast(nullif(trim(a.City), '') as string)              as city_name
            , cast(a.StateProvinceID as number(38,0))               as state_province_id
            , cast(nullif(trim(s.StateProvinceName), '') as string) as state_province_name
            , cast(nullif(trim(s.StateProvinceCode), '') as string) as state_province_code
            , cast(nullif(trim(s.CountryRegionCode), '') as string) as country_region_code
            , cast(nullif(trim(c.CountryRegionName), '') as string) as country_region_name
        from address a
        inner join stateprovince s
            on s.StateProvinceID = a.StateProvinceID
        inner join country c
            on c.CountryRegionCode = s.CountryRegionCode
    )

select
    address_id
  , city_name
  , state_province_id
  , state_province_name
  , state_province_code
  , country_region_code
  , country_region_name
from joined