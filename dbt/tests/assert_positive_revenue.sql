-- assert_positive_revenue.sql
-- Custom test: Ensure no daily revenue records have negative net revenue.
-- A negative value would indicate a data quality issue in upstream sources.
-- This test PASSES when it returns 0 rows.

select
    order_date,
    net_revenue
from {{ ref('mart_daily_revenue') }}
where net_revenue < 0
