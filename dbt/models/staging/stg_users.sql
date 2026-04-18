with source as (
    select * from {{ source('raw', 'users') }}
),

staged as (
    select
        -- Primary key
        id as user_id,

        -- Personal info
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        trim(coalesce(maiden_name, '')) as maiden_name,
        age,
        lower(trim(coalesce(gender, 'unknown'))) as gender,

        -- Contact
        lower(trim(email)) as email,
        trim(coalesce(phone, '')) as phone,
        lower(trim(coalesce(username, ''))) as username,

        -- Demographics
        birth_date,

        -- Address (already flattened in ingestion, standardize here)
        trim(coalesce(address_street, '')) as address_street,
        trim(coalesce(address_city, '')) as address_city,
        trim(coalesce(address_state, '')) as address_state,
        trim(coalesce(address_postal_code, '')) as address_postal_code,
        trim(coalesce(address_country, 'Unknown')) as address_country,
        -- Full address for display
        concat_ws(', ',
            nullif(trim(address_street), ''),
            nullif(trim(address_city), ''),
            nullif(trim(address_state), ''),
            nullif(trim(address_postal_code), '')
        ) as full_address,

        -- Company info
        trim(coalesce(company_name, '')) as company_name,
        trim(coalesce(company_title, '')) as company_title,
        trim(coalesce(company_department, '')) as company_department,

        -- Education
        trim(coalesce(university, '')) as university,

        -- Audit
        ingested_at

    from source
)

select * from staged
