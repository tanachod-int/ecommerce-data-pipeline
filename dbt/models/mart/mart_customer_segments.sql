with orders as (
    select * from {{ ref('stg_orders') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

customer_metrics as (
    select
        o.user_id,
        u.full_name,
        u.email,
        u.gender,
        u.age,
        u.address_city,
        u.address_state,
        u.address_country,

        -- Order behavior
        count(distinct o.order_id) as total_orders,
        count(o.order_line_id) as total_line_items,
        sum(o.quantity) as total_units_purchased,

        -- Spending
        sum(o.discounted_total) as total_spent,
        round(avg(o.discounted_total), 2) as avg_line_item_value,
        round(
            sum(o.discounted_total) / nullif(count(distinct o.order_id), 0),
            2
        ) as avg_order_value,
        max(o.discounted_total) as max_single_line_item,

        -- Discounts
        sum(o.discount_amount) as total_discount_received,
        round(avg(o.discount_percentage), 2) as avg_discount_pct,

        -- Time behavior
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        max(o.order_date) - min(o.order_date) as customer_lifespan_days,

        -- Product diversity
        count(distinct o.product_id) as unique_products_bought

    from orders o
    left join users u
        on o.user_id = u.user_id
    group by
        o.user_id,
        u.full_name,
        u.email,
        u.gender,
        u.age,
        u.address_city,
        u.address_state,
        u.address_country
),

segmented as (
    select
        *,

        -- RFM-inspired segmentation based on spending tiers
        case
            when total_spent >= (
                select percentile_cont(0.80) within group (order by total_spent)
                from customer_metrics
            ) then 'VIP'
            when total_spent >= (
                select percentile_cont(0.50) within group (order by total_spent)
                from customer_metrics
            ) then 'Regular'
            when total_orders >= 2 then 'Occasional'
            else 'One-time'
        end as customer_segment,

        -- Spending rank
        row_number() over (order by total_spent desc) as spending_rank

    from customer_metrics
)

select * from segmented
