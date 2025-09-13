-- Fails if the sum of gross_amount for 2011 deviates from the expected value (tolerance of 0.01)
-- CEO audited Gross Sales 2011 = 12,646,112.16
-- Validate from fct_sales, rounding only at aggregate level.

with calc as (
    select
        round(sum(f.gross_amount), 2) as total_gross_2011
    from {{ ref('fct_sales') }} f
    join {{ ref('dim_date') }} d
        on d.date_key = f.order_date_key
    where d.year = 2011
)

select total_gross_2011
from calc
where abs(total_gross_2011 - 12646112.16) > 0.01
