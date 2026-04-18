with orders as (
    select * from {{ ref('stg_orders') }}
),

daily_metrics as (
    select
        order_date,
        order_year,
        order_month,
        order_day_of_week,

        -- Revenue metrics
        count(distinct order_id) as total_orders,
        count(order_line_id) as total_line_items,
        sum(quantity) as total_units_sold,

        -- Financial metrics
        sum(line_total) as gross_revenue,
        sum(discounted_total) as net_revenue,
        sum(discount_amount) as total_discount_given,

        -- Averages
        round(avg(discounted_total), 2) as avg_order_line_value,
        round(
            sum(discounted_total) / nullif(count(distinct order_id), 0),
            2
        ) as avg_order_value,

        -- Customer reach
        count(distinct user_id) as unique_customers

    from orders
    group by
        order_date,
        order_year,
        order_month,
        order_day_of_week
)

select * from daily_metrics
order by order_date
