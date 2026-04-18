with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        o.product_id,
        p.product_title,
        p.product_category,
        p.brand,
        p.price as current_price,
        p.rating,

        -- Sales volume
        count(distinct o.order_id) as times_ordered,
        count(o.order_line_id) as total_line_items,
        sum(o.quantity) as total_units_sold,

        -- Revenue
        sum(o.line_total) as gross_revenue,
        sum(o.discounted_total) as net_revenue,
        sum(o.discount_amount) as total_discount_given,

        -- Averages
        round(avg(o.unit_price), 2) as avg_selling_price,
        round(avg(o.discount_percentage), 2) as avg_discount_pct,

        -- Time range
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date

    from orders o
    left join products p
        on o.product_id = p.product_id
    group by
        o.product_id,
        p.product_title,
        p.product_category,
        p.brand,
        p.price,
        p.rating
),

ranked as (
    select
        *,
        row_number() over (order by net_revenue desc) as revenue_rank,
        row_number() over (order by total_units_sold desc) as units_sold_rank,
        row_number() over (
            partition by product_category
            order by net_revenue desc
        ) as category_rank

    from product_sales
)

select * from ranked
