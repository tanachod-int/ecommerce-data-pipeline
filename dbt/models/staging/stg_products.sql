with source as (
    select * from {{ source('raw', 'products') }}
),

staged as (
    select
        -- Primary key
        id as product_id,

        -- Product info
        trim(title) as product_title,
        trim(coalesce(description, '')) as product_description,
        lower(trim(category)) as product_category,
        trim(coalesce(brand, 'Unknown')) as brand,
        upper(trim(coalesce(sku, ''))) as sku,

        -- Pricing
        cast(price as numeric(10,2)) as price,
        cast(coalesce(discount_percentage, 0) as numeric(5,2)) as discount_percentage,
        -- Calculated: price after discount
        round(
            cast(price as numeric(10,2))
            * (1 - cast(coalesce(discount_percentage, 0) as numeric(5,2)) / 100),
            2
        ) as discounted_price,

        -- Product attributes
        cast(coalesce(rating, 0) as numeric(3,2)) as rating,
        coalesce(stock, 0) as stock_quantity,
        cast(coalesce(weight, 0) as numeric(10,2)) as weight_kg,
        coalesce(minimum_order_quantity, 1) as min_order_qty,

        -- Status & policies
        trim(coalesce(availability_status, 'Unknown')) as availability_status,
        trim(coalesce(warranty_information, 'No warranty')) as warranty_info,
        trim(coalesce(shipping_information, 'Standard shipping')) as shipping_info,
        trim(coalesce(return_policy, 'No return policy')) as return_policy,

        -- Media (keep as JSON text)
        thumbnail as thumbnail_url,
        images as image_urls_json,
        tags as tags_json,

        -- Audit
        ingested_at

    from source
)

select * from staged
