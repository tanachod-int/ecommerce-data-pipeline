with source as (
    select * from {{ source('raw', 'orders') }}
),

staged as (
    select
        -- Primary key
        id as order_line_id,

        -- Order identifiers
        cart_id as order_id,
        user_id,
        product_id,

        -- Product snapshot (denormalized at time of order)
        trim(coalesce(product_title, '')) as product_title,

        -- Pricing
        cast(price as numeric(10,2)) as unit_price,
        quantity,
        cast(total as numeric(12,2)) as line_total,
        cast(coalesce(discount_percentage, 0) as numeric(5,2)) as discount_percentage,
        cast(coalesce(discounted_total, total) as numeric(12,2)) as discounted_total,

        -- Calculated: actual discount amount
        cast(total as numeric(12,2))
            - cast(coalesce(discounted_total, total) as numeric(12,2)) as discount_amount,

        -- Date dimension
        order_date,
        extract(year from order_date)::int as order_year,
        extract(month from order_date)::int as order_month,
        extract(dow from order_date)::int as order_day_of_week,

        -- Audit
        ingested_at

    from source
    where order_date is not null
)

select * from staged
