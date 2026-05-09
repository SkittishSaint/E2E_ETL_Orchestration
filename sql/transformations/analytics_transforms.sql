-- analytics_transforms.sql
-- Builds analytics-ready dimension and fact tables from the staging layer.
-- This script also uses cluster keys to optimize time-series queries on order_date and sale_date.

CREATE SCHEMA IF NOT EXISTS ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_CUSTOMERS
CLUSTER BY (user_id) AS
SELECT DISTINCT
    user_id,
    email,
    username,
    first_name,
    last_name,
    full_name,
    address_city,
    address_state,
    address_postal_code,
    sk_user
FROM STAGING.STG_USERS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_PRODUCTS
CLUSTER BY (product_id) AS
SELECT DISTINCT
    product_id,
    product_name,
    price,
    category,
    brand,
    status,
    is_active,
    sk_product
FROM STAGING.STG_PRODUCTS;

CREATE OR REPLACE TABLE ANALYTICS.FACT_ORDERS
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
    sk_order
FROM STAGING.STG_ORDERS;

CREATE OR REPLACE TABLE ANALYTICS.AGG_DAILY_SALES
CLUSTER BY (sale_date) AS
SELECT
    DATE_TRUNC('DAY', order_date) AS sale_date,
    COUNT(order_id) AS order_count,
    SUM(total) AS total_revenue_usd,
    AVG(total) AS avg_order_value_usd,
    COUNT(DISTINCT user_id) AS unique_customers
FROM STAGING.STG_ORDERS
GROUP BY 1
ORDER BY 1;
