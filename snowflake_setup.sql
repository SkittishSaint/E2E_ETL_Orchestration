-- ============================================================================
-- snowflake_setup.sql
-- ============================================================================
-- Snowflake schema and table setup for E2E ETL Orchestration project
-- Run this script in Snowflake to prepare the data warehouse
-- ============================================================================

-- ============================================================================
-- PART 1: Create Databases and Schemas
-- ============================================================================

CREATE DATABASE IF NOT EXISTS ETL_DB;
CREATE DATABASE IF NOT EXISTS CONTROL_DB;

-- Create RAW schema for ingested data
CREATE SCHEMA IF NOT EXISTS ETL_DB.RAW;

-- Create STAGING schema for transformed data
CREATE SCHEMA IF NOT EXISTS ETL_DB.STAGING;

-- Create ANALYTICS schema for reporting
CREATE SCHEMA IF NOT EXISTS ETL_DB.ANALYTICS;

-- Create CONTROL schema for monitoring and watermarks
CREATE SCHEMA IF NOT EXISTS CONTROL_DB.CONTROL;

-- ============================================================================
-- PART 2: Create Watermark Table (for incremental loading)
-- ============================================================================

CREATE TABLE IF NOT EXISTS CONTROL_DB.CONTROL.ETL_WATERMARKS (
    pipeline_name VARCHAR(255) NOT NULL,
    watermark_value TIMESTAMP_TZ,
    status VARCHAR(50) DEFAULT 'SUCCESS',
    rows_loaded INT,
    recorded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (pipeline_name, recorded_at)
);

-- ============================================================================
-- PART 3: Create RAW Layer Tables (Ingested Data)
-- ============================================================================

-- Orders raw table
CREATE TABLE IF NOT EXISTS ETL_DB.RAW.ORDERS_RAW (
    order_id INT NOT NULL,
    user_id INT NOT NULL,
    products_json VARCHAR(16777216),
    total DECIMAL(10,2),
    discounted_total DECIMAL(10,2),
    total_products INT,
    total_quantity INT,
    order_value DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    avg_product_value DECIMAL(10,2),
    order_date TIMESTAMP_TZ,
    updated_at TIMESTAMP_TZ,
    sk_order VARCHAR(16),
    _transformed_at VARCHAR(255),
    _pipeline_version VARCHAR(10),
    _extracted_at VARCHAR(255),
    _source VARCHAR(500),
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (updated_at, order_id);

-- Users raw table
CREATE TABLE IF NOT EXISTS ETL_DB.RAW.USERS_RAW (
    id INT,
    user_id INT NOT NULL,
    email VARCHAR(255),
    username VARCHAR(255),
    password_masked VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    address_line1 VARCHAR(255),
    address_city VARCHAR(100),
    address_state VARCHAR(50),
    address_postal_code VARCHAR(20),
    address_lat DECIMAL(11,8),
    address_lng DECIMAL(11,8),
    phone_masked VARCHAR(20),
    sk_user VARCHAR(16),
    _transformed_at VARCHAR(255),
    _pipeline_version VARCHAR(10),
    _extracted_at VARCHAR(255),
    _source VARCHAR(500),
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (user_id);

-- Products raw table
CREATE TABLE IF NOT EXISTS ETL_DB.RAW.PRODUCTS_RAW (
    id INT,
    product_id INT NOT NULL,
    product_name VARCHAR(255),
    description VARCHAR(2000),
    rating DECIMAL(3,2),
    reviews INT,
    price DECIMAL(10,2),
    discount_percentage DECIMAL(5,2),
    discounted_price DECIMAL(10,2),
    stock INT,
    category VARCHAR(100),
    thumbnail VARCHAR(2000),
    sku VARCHAR(100),
    weight DECIMAL(10,2),
    dimensions VARCHAR(255),
    warranty_months INT,
    return_policy VARCHAR(500),
    sk_product VARCHAR(16),
    _transformed_at VARCHAR(255),
    _pipeline_version VARCHAR(10),
    _extracted_at VARCHAR(255),
    _source VARCHAR(500),
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (product_id, category);

-- ============================================================================
-- PART 4: Create STAGING Layer Tables (Cleaned & Enriched Data)
-- ============================================================================

-- Staging orders table
CREATE TABLE IF NOT EXISTS ETL_DB.STAGING.STG_ORDERS (
    sk_order VARCHAR(16) PRIMARY KEY,
    order_id INT NOT NULL,
    user_id INT NOT NULL,
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(10,2),
    product_count INT,
    total_quantity INT,
    order_date DATE,
    created_at TIMESTAMP_TZ,
    updated_at TIMESTAMP_TZ,
    dbt_updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    dbt_valid_from TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    dbt_valid_to TIMESTAMP_TZ
)
CLUSTER BY (order_date, user_id);

-- Staging users table
CREATE TABLE IF NOT EXISTS ETL_DB.STAGING.STG_USERS (
    sk_user VARCHAR(16) PRIMARY KEY,
    user_id INT NOT NULL,
    email VARCHAR(255),
    username VARCHAR(255),
    full_name VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    dbt_updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    dbt_valid_from TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    dbt_valid_to TIMESTAMP_TZ
)
CLUSTER BY (user_id);

-- Staging products table
CREATE TABLE IF NOT EXISTS ETL_DB.STAGING.STG_PRODUCTS (
    sk_product VARCHAR(16) PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(255),
    description VARCHAR(2000),
    rating DECIMAL(3,2),
    reviews INT,
    category VARCHAR(100),
    price DECIMAL(10,2),
    discount_percentage DECIMAL(5,2),
    discounted_price DECIMAL(10,2),
    stock INT,
    sku VARCHAR(100),
    weight DECIMAL(10,2),
    dimensions VARCHAR(255),
    return_policy VARCHAR(500),
    _transformed_at VARCHAR(255),
    _pipeline_version VARCHAR(10),
    dbt_updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    dbt_valid_from TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    dbt_valid_to TIMESTAMP_TZ
)
CLUSTER BY (category, product_id);

-- ============================================================================
-- PART 5: Create ANALYTICS Layer Tables (For Reporting)
-- ============================================================================

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.DIM_CUSTOMERS (
    dim_customer_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    email VARCHAR(255),
    full_name VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    total_orders INT DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0,
    first_order_date DATE,
    last_order_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    dbt_updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (user_id);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.DIM_PRODUCTS (
    dim_product_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    discount_percentage DECIMAL(5,2),
    discounted_price DECIMAL(10,2),
    stock INT,
    dbt_updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (category);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.FACT_ORDERS (
    fact_order_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    dim_customer_id INT,
    dim_product_id INT,
    order_date DATE,
    order_amount DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(12,2),
    quantity INT,
    FOREIGN KEY (dim_customer_id) REFERENCES ETL_DB.ANALYTICS.DIM_CUSTOMERS(dim_customer_id),
    FOREIGN KEY (dim_product_id) REFERENCES ETL_DB.ANALYTICS.DIM_PRODUCTS(dim_product_id)
)
CLUSTER BY (order_date, dim_customer_id);

-- Aggregation: Daily Sales
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.AGG_DAILY_SALES (
    sale_date DATE PRIMARY KEY,
    total_orders INT,
    total_customers INT,
    total_amount DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    total_discount DECIMAL(10,2),
    cancelled_orders INT,
    cancellation_rate_pct DECIMAL(5,2),
    dbt_updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (sale_date);

-- ============================================================================
-- PART 7: Create Storage Integration (For external data loading)
-- ============================================================================

-- Optional: If you plan to load from S3/Azure Blob Storage
-- CREATE STORAGE INTEGRATION IF NOT EXISTS s3_integration
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = S3
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-role'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/');

-- ============================================================================
-- PART 8: Create Roles and Grant Permissions
-- ============================================================================

-- Create ETL role
CREATE ROLE IF NOT EXISTS ETL_ROLE;

-- Grant permissions on databases
GRANT USAGE ON DATABASE ETL_DB TO ROLE ETL_ROLE;
GRANT USAGE ON DATABASE CONTROL_DB TO ROLE ETL_ROLE;

-- Grant permissions on schemas
GRANT USAGE ON SCHEMA ETL_DB.RAW TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA ETL_DB.STAGING TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA ETL_DB.ANALYTICS TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA CONTROL_DB.CONTROL TO ROLE ETL_ROLE;

-- Grant table permissions
GRANT ALL ON ALL TABLES IN SCHEMA ETL_DB.RAW TO ROLE ETL_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA ETL_DB.STAGING TO ROLE ETL_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CONTROL_DB.CONTROL TO ROLE ETL_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ETL_DB.ANALYTICS TO ROLE ETL_ROLE;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Grant warehouse permissions
GRANT ALL ON WAREHOUSE ETL_WH TO ROLE ETL_ROLE;

-- ============================================================================
-- PART 9: Verify Setup
-- ============================================================================

-- Check databases
SHOW DATABASES LIKE 'ETL%';

-- Check schemas
SHOW SCHEMAS IN ETL_DB;

-- Check tables in RAW schema
SHOW TABLES IN ETL_DB.RAW;

-- Check watermark table
SHOW TABLES IN CONTROL_DB.CONTROL;

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. Replace 'your_username' with actual Snowflake username before running
-- 2. Ensure you have ACCOUNTADMIN or similar privileges to create databases
-- 3. Run this once to set up the initial schema
-- 4. Clustering keys are set for optimal query performance
-- 5. Update Airflow connection with these credentials:
--    - Account: your_account_id
--    - User: your_username
--    - Password: your_password
--    - Database: ETL_DB
--    - Warehouse: ETL_WH
--    - Schema: RAW (or appropriate schema)
--    - Role: ETL_ROLE
-- ============================================================================
