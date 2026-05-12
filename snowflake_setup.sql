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

-- Pipeline run metrics table for monitoring and performance reporting
CREATE TABLE IF NOT EXISTS CONTROL_DB.CONTROL.PIPELINE_RUN_METRICS (
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255),
    run_id VARCHAR(500),
    run_type VARCHAR(100),
    status VARCHAR(50),
    rows_loaded INT DEFAULT 0,
    duration_seconds FLOAT,
    error_message VARCHAR(5000),
    recorded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (dag_id, recorded_at);

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
    order_id INT NOT NULL,
    user_id INT NOT NULL,
    products_json VARCHAR(16777216),
    total DECIMAL(10,2),
    discounted_total DECIMAL(10,2),
    total_products INT,
    total_quantity INT,
    order_date TIMESTAMP_TZ,
    updated_at TIMESTAMP_TZ,
    sk_order VARCHAR(16),
    _transformed_at VARCHAR(255),
    _pipeline_version VARCHAR(10)
)
CLUSTER BY (order_date);

-- Staging users table
CREATE TABLE IF NOT EXISTS ETL_DB.STAGING.STG_USERS (
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
    _pipeline_version VARCHAR(10)
)
CLUSTER BY (user_id);

-- Staging products table
CREATE TABLE IF NOT EXISTS ETL_DB.STAGING.STG_PRODUCTS (
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
    sku VARCHAR(100),
    weight DECIMAL(10,2),
    dimensions VARCHAR(255),
    warranty_months INT,
    return_policy VARCHAR(500),
    sk_product VARCHAR(16),
    _transformed_at VARCHAR(255),
    _pipeline_version VARCHAR(10)
)
CLUSTER BY (product_id);

-- ============================================================================
-- PART 5: Create ANALYTICS Layer Tables (For Reporting)
-- ============================================================================

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.DIM_CUSTOMERS (
    user_id INT NOT NULL,
    email VARCHAR(255),
    username VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    address_city VARCHAR(100),
    address_state VARCHAR(50),
    address_postal_code VARCHAR(20),
    sk_user VARCHAR(16)
)
CLUSTER BY (user_id);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.DIM_PRODUCTS (
    product_id INT NOT NULL,
    product_name VARCHAR(255),
    price DECIMAL(10,2),
    category VARCHAR(100),
    discount_percentage DECIMAL(5,2),
    discounted_price DECIMAL(10,2),
    stock INT,
    warranty_months INT,
    sk_product VARCHAR(16)
)
CLUSTER BY (product_id);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.FACT_ORDERS (
    order_id INT NOT NULL,
    user_id INT,
    products_json VARCHAR(16777216),
    total DECIMAL(10,2),
    discounted_total DECIMAL(10,2),
    total_products INT,
    total_quantity INT,
    order_date TIMESTAMP_TZ,
    updated_at TIMESTAMP_TZ,
    sk_order VARCHAR(16)
)
CLUSTER BY (order_date);

-- Aggregation: Daily Sales
CREATE TABLE IF NOT EXISTS ETL_DB.ANALYTICS.AGG_DAILY_SALES (
    sale_date DATE,
    order_count INT,
    total_revenue_usd DECIMAL(12,2),
    avg_order_value_usd DECIMAL(12,2),
    unique_customers INT
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

-- Grant object creation privileges required by write_pandas staging, MERGE upserts,
-- and CREATE OR REPLACE TABLE transformation scripts.
GRANT CREATE TABLE ON SCHEMA ETL_DB.RAW TO ROLE ETL_ROLE;
GRANT CREATE STAGE ON SCHEMA ETL_DB.RAW TO ROLE ETL_ROLE;
GRANT CREATE FILE FORMAT ON SCHEMA ETL_DB.RAW TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA ETL_DB.STAGING TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA ETL_DB.ANALYTICS TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA CONTROL_DB.CONTROL TO ROLE ETL_ROLE;

-- Grant table permissions
GRANT ALL ON ALL TABLES IN SCHEMA ETL_DB.RAW TO ROLE ETL_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA ETL_DB.STAGING TO ROLE ETL_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA ETL_DB.ANALYTICS TO ROLE ETL_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CONTROL_DB.CONTROL TO ROLE ETL_ROLE;

-- Keep grants available for objects created or replaced by the ETL role.
GRANT ALL ON FUTURE TABLES IN SCHEMA ETL_DB.RAW TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ETL_DB.STAGING TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ETL_DB.ANALYTICS TO ROLE ETL_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CONTROL_DB.CONTROL TO ROLE ETL_ROLE;

-- Transfer ownership of setup-time tables that ETL_ROLE replaces during runs.
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ETL_DB.STAGING TO ROLE ETL_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ETL_DB.ANALYTICS TO ROLE ETL_ROLE COPY CURRENT GRANTS;

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
-- 5. CONTROL_DB.CONTROL.PIPELINE_RUN_METRICS stores pipeline performance and failure metrics
-- 6. Update Airflow connection with these credentials:
--    - Account: your_account_id
--    - User: your_username
--    - Password: your_password
--    - Database: ETL_DB
--    - Warehouse: ETL_WH
--    - Schema: RAW (or appropriate schema)
--    - Role: ETL_ROLE
-- ============================================================================
