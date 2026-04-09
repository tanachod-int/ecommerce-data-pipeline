-- Database Initialization — Creates schemas and raw tables for the pipeline.
-- Runs automatically when PostgreSQL container starts.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- Raw Products (from DummyJSON /products)
CREATE TABLE IF NOT EXISTS raw.products (
    id                      INTEGER PRIMARY KEY,
    title                   VARCHAR(500),
    description             TEXT,
    category                VARCHAR(100),
    price                   NUMERIC(10,2),
    discount_percentage     NUMERIC(5,2),
    rating                  NUMERIC(3,2),
    stock                   INTEGER,
    brand                   VARCHAR(200),
    sku                     VARCHAR(50),
    weight                  NUMERIC(10,2),
    warranty_information    VARCHAR(200),
    shipping_information    VARCHAR(200),
    availability_status     VARCHAR(50),
    return_policy           VARCHAR(200),
    minimum_order_quantity  INTEGER,
    thumbnail               TEXT,
    images                  TEXT,
    tags                    TEXT,
    ingested_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw Users (from DummyJSON /users)
CREATE TABLE IF NOT EXISTS raw.users (
    id                  INTEGER PRIMARY KEY,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    maiden_name         VARCHAR(100),
    age                 INTEGER,
    gender              VARCHAR(20),
    email               VARCHAR(200),
    phone               VARCHAR(50),
    username            VARCHAR(100),
    birth_date          DATE,
    address_street      VARCHAR(300),
    address_city        VARCHAR(100),
    address_state       VARCHAR(100),
    address_postal_code VARCHAR(20),
    address_country     VARCHAR(100),
    company_name        VARCHAR(200),
    company_title       VARCHAR(200),
    company_department  VARCHAR(100),
    university          VARCHAR(300),
    ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw Orders (flattened from DummyJSON /carts — each row = one line item)
CREATE TABLE IF NOT EXISTS raw.orders (
    id                  SERIAL PRIMARY KEY,
    cart_id             INTEGER NOT NULL,
    user_id             INTEGER NOT NULL,
    product_id          INTEGER NOT NULL,
    product_title       VARCHAR(500),
    price               NUMERIC(10,2),
    quantity            INTEGER,
    total               NUMERIC(12,2),
    discount_percentage NUMERIC(5,2),
    discounted_total    NUMERIC(12,2),
    order_date          DATE,
    ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON raw.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_product_id ON raw.orders(product_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON raw.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_products_category ON raw.products(category);
