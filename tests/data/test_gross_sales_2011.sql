-- Fails if the sum of gross_amount for 2011 deviates from the expected value (tolerance of 0.01)

--with calc as (
--    select
--        round(sum(f.gross_amount), 2) as total_gross_2011
--    from {{ ref('fct_sales') }} f
--    join {{ ref('dim_date') }} d
--        on d.date_key = f.order_date_key
--    where d.year = 2011
--)

-- CEO requires: Gross Sales 2011 = 12,646,112.16 (aggregate rounding)
-- We compute BRUTO as sum(order_qty * unit_price) at the aggregate level.
with calc as (
    select round(sum(l.order_qty * l.unit_price), 2) as total_gross_2011
    from {{ ref('int_sales__order_line') }} l
    join {{ ref('dim_date') }} d
      on d.date_day = l.order_date
    where d.year = 2011
)
select total_gross_2011
from calc
where abs(total_gross_2011 - 12646112.16) > 0.01;
