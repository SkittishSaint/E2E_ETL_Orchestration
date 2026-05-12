-- staging_transforms.sql
-- Transforms raw Snowflake tables into a staging layer for analytics.
-- Clustering keys are applied to support faster range and join performance.

CREATE SCHEMA IF NOT EXISTS STAGING;

CREATE OR REPLACE TABLE STAGING.STG_USERS
CLUSTER BY (user_id) AS
SELECT
    user_id,
    email,
    username,
    password_masked,
    first_name,
    last_name,
    full_name,
    address_line1,
    address_city,
    address_state,
    address_postal_code,
    address_lat,
    address_lng,
    phone_masked,
    sk_user,
    _transformed_at,
    _pipeline_version
FROM RAW.USERS_RAW;

CREATE OR REPLACE TABLE STAGING.STG_PRODUCTS
CLUSTER BY (product_id) AS
SELECT
    product_id,
    product_name,
    description,
    rating,
    reviews,
    price,
    discount_percentage,
    discounted_price,
    stock,
    category,
    sku,
    weight,
    dimensions,
    warranty_months,
    return_policy,
    sk_product,
    _transformed_at,
    _pipeline_version
FROM RAW.PRODUCTS_RAW;

CREATE OR REPLACE TABLE STAGING.STG_ORDERS
CLUSTER BY (order_date) AS
SELECT
    order_id,
    user_id,
    products_json,
    total,
    discounted_total,
    total_products,
    total_quantity,
    order_date,
    updated_at,
    sk_order,
    _transformed_at,
    _pipeline_version
FROM RAW.ORDERS_RAW;
