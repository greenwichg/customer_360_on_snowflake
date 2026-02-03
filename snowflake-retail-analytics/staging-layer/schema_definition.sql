/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - STAGING LAYER SCHEMA DEFINITION
================================================================================
Purpose: Create staging tables for cleansed and validated data (ODS)
Concepts: Transient tables, data types, constraints

Interview Points:
- Staging tables use proper data types (unlike raw VARCHAR landing)
- Transient tables reduce storage costs (no Fail-safe)
- This is the Operational Data Store (ODS) layer
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA STAGING;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: STAGING TABLES (TRANSIENT - Cost Optimized)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 Staged Sales Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE STG_SALES (
    -- Business keys
    order_id VARCHAR(50) NOT NULL,
    order_line_id INTEGER NOT NULL,
    -- Foreign keys
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    store_id VARCHAR(50),
    -- Measures
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    total_amount DECIMAL(12,2),
    -- Attributes
    transaction_date TIMESTAMP_NTZ,
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    -- Audit columns
    source_file VARCHAR(500),
    file_row_number INTEGER,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    -- Data quality flags
    dq_is_valid BOOLEAN DEFAULT TRUE,
    dq_error_details VARCHAR
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Staged sales data with proper types and validation flags';

-- Add clustering for better performance (optional for staging)
-- ALTER TABLE STG_SALES CLUSTER BY (transaction_date);

-- -----------------------------------------------------------------------------
-- 1.2 Staged Products Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE STG_PRODUCTS (
    -- Business key
    product_id VARCHAR(50) NOT NULL,
    -- Attributes
    product_name VARCHAR(500),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(200),
    -- Measures
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    weight_kg DECIMAL(8,3),
    -- Flags
    is_active BOOLEAN,
    launch_date DATE,
    -- Nested attributes (kept as VARIANT for flexibility)
    attributes VARIANT,
    -- Audit columns
    source_file VARCHAR(500),
    file_row_number INTEGER,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    -- Data quality flags
    dq_is_valid BOOLEAN DEFAULT TRUE,
    dq_error_details VARCHAR
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Staged product catalog with proper types';

-- -----------------------------------------------------------------------------
-- 1.3 Staged Customers Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE STG_CUSTOMERS (
    -- Business key
    customer_id VARCHAR(50) NOT NULL,
    -- PII fields
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    -- Demographics
    date_of_birth DATE,
    gender VARCHAR(10),
    -- Business attributes
    registration_date DATE,
    customer_segment VARCHAR(50),
    loyalty_points INTEGER,
    preferred_contact VARCHAR(50),
    is_active BOOLEAN,
    -- Address
    address_line1 VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    -- Audit columns
    source_file VARCHAR(500),
    file_row_number INTEGER,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    -- Data quality flags
    dq_is_valid BOOLEAN DEFAULT TRUE,
    dq_error_details VARCHAR,
    -- Hash for change detection (SCD)
    record_hash VARCHAR(64)
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Staged customer data with hash for change detection';

-- -----------------------------------------------------------------------------
-- 1.4 Staged Clickstream Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE STG_CLICKSTREAM (
    -- Business keys
    event_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(100),
    -- Foreign keys
    customer_id VARCHAR(50),  -- NULL for anonymous visitors
    -- Event details
    event_timestamp TIMESTAMP_NTZ,
    event_type VARCHAR(50),
    page_url VARCHAR(2000),
    referrer_url VARCHAR(2000),
    -- Device info
    device_type VARCHAR(50),
    browser VARCHAR(100),
    os VARCHAR(100),
    ip_address VARCHAR(50),
    -- Engagement metrics
    duration_seconds INTEGER,
    scroll_depth_percent INTEGER,
    -- E-commerce events
    product_id VARCHAR(50),
    quantity INTEGER,
    order_id VARCHAR(50),
    order_value DECIMAL(12,2),
    cart_value DECIMAL(12,2),
    search_query VARCHAR(500),
    -- Audit columns
    source_file VARCHAR(500),
    file_row_number INTEGER,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    -- Data quality
    dq_is_valid BOOLEAN DEFAULT TRUE,
    dq_error_details VARCHAR
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Staged clickstream events with proper types';

-- -----------------------------------------------------------------------------
-- 1.5 Staged Stores Table (Reference Data)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE STG_STORES (
    store_id VARCHAR(50) NOT NULL,
    store_name VARCHAR(200),
    store_type VARCHAR(50),  -- RETAIL, WAREHOUSE, OUTLET
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    manager_name VARCHAR(200),
    open_date DATE,
    close_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    -- Audit
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dq_is_valid BOOLEAN DEFAULT TRUE
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Staged store/location reference data';

-- Insert sample store data (reference data is often manually loaded)
INSERT INTO STG_STORES (store_id, store_name, store_type, city, state, region, country, is_active, open_date)
VALUES
    ('STORE-001', 'Downtown Flagship', 'RETAIL', 'New York', 'NY', 'Northeast', 'USA', TRUE, '2020-01-15'),
    ('STORE-002', 'West Coast Hub', 'RETAIL', 'Los Angeles', 'CA', 'West', 'USA', TRUE, '2020-03-20'),
    ('STORE-003', 'Central Distribution', 'WAREHOUSE', 'Chicago', 'IL', 'Midwest', 'USA', TRUE, '2019-06-01'),
    ('STORE-004', 'Tech Valley Store', 'RETAIL', 'San Francisco', 'CA', 'West', 'USA', TRUE, '2021-02-10'),
    ('STORE-005', 'Outlet Mall Location', 'OUTLET', 'Dallas', 'TX', 'South', 'USA', TRUE, '2021-08-15');

-- =============================================================================
-- SECTION 2: DATA QUALITY REJECTION TABLES
-- =============================================================================
/*
Store rejected/invalid records for investigation and reprocessing.
*/

CREATE OR REPLACE TRANSIENT TABLE STG_SALES_REJECTED (
    -- Original raw data
    raw_order_id VARCHAR,
    raw_order_line_id VARCHAR,
    raw_customer_id VARCHAR,
    raw_product_id VARCHAR,
    raw_store_id VARCHAR,
    raw_transaction_date VARCHAR,
    raw_quantity VARCHAR,
    raw_unit_price VARCHAR,
    raw_discount_percent VARCHAR,
    raw_total_amount VARCHAR,
    raw_payment_method VARCHAR,
    raw_order_status VARCHAR,
    -- Rejection details
    rejection_reason VARCHAR(1000),
    rejection_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500),
    file_row_number INTEGER
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Rejected sales records for investigation';

CREATE OR REPLACE TRANSIENT TABLE STG_CUSTOMERS_REJECTED (
    raw_customer_id VARCHAR,
    raw_email VARCHAR,
    raw_phone VARCHAR,
    rejection_reason VARCHAR(1000),
    rejection_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500),
    file_row_number INTEGER
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Rejected customer records for investigation';

-- =============================================================================
-- SECTION 3: STAGING METADATA TABLES
-- =============================================================================

-- Track load batches for auditing
CREATE OR REPLACE TABLE STG_LOAD_AUDIT (
    batch_id VARCHAR(50) DEFAULT UUID_STRING(),
    table_name VARCHAR(100),
    source_stage VARCHAR(200),
    file_pattern VARCHAR(500),
    load_start_time TIMESTAMP_NTZ,
    load_end_time TIMESTAMP_NTZ,
    rows_loaded INTEGER,
    rows_rejected INTEGER,
    status VARCHAR(50),  -- STARTED, COMPLETED, FAILED
    error_message VARCHAR(2000),
    PRIMARY KEY (batch_id)
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Audit log for staging layer load operations';

-- Track file processing status
CREATE OR REPLACE TABLE STG_FILE_REGISTRY (
    file_path VARCHAR(1000) NOT NULL,
    file_name VARCHAR(500),
    file_size_bytes INTEGER,
    file_modified_date TIMESTAMP_NTZ,
    process_status VARCHAR(50),  -- PENDING, PROCESSING, LOADED, FAILED
    process_timestamp TIMESTAMP_NTZ,
    batch_id VARCHAR(50),
    rows_loaded INTEGER,
    error_message VARCHAR(2000),
    PRIMARY KEY (file_path)
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Registry of processed files for incremental loading';

-- =============================================================================
-- SECTION 4: VERIFY STAGING SCHEMA
-- =============================================================================

-- Show all tables in staging
SHOW TABLES IN SCHEMA STAGING;

-- Get table details
SELECT
    TABLE_NAME,
    TABLE_TYPE,
    IS_TRANSIENT,
    ROW_COUNT,
    BYTES,
    RETENTION_TIME,
    CREATED,
    COMMENT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'STAGING'
ORDER BY TABLE_NAME;

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: Why use TRANSIENT tables for staging?
A1:
    - Staging data can be reloaded from source if lost
    - No Fail-safe = ~50% storage cost savings
    - 1-day Time Travel is usually sufficient
    - Intermediate layer doesn't need long-term protection

Q2: Why store data as VARCHAR in landing but typed in staging?
A2:
    LANDING (VARCHAR):
    - Accept any data without failing
    - Preserve original values for debugging
    - Flexible for schema changes

    STAGING (Typed):
    - Data validated and converted
    - Better query performance
    - Enables proper aggregations
    - Catches type errors early

Q3: What's the purpose of the record_hash column?
A3:
    - Enables efficient change detection (SCD Type 2)
    - Hash all business columns (exclude audit columns)
    - Compare incoming hash vs existing hash
    - If different → record has changed → create new version
    - More efficient than comparing every column

Q4: How do you handle data quality in staging?
A4:
    1. dq_is_valid flag marks records passing validation
    2. dq_error_details captures validation failures
    3. Rejected records go to _REJECTED tables
    4. Valid records flow to curated layer
    5. Audit tables track batch results

Q5: What's the difference between ODS and Data Warehouse?
A5:
    ODS (Staging):
    - Current/recent data
    - Subject-oriented but not dimensional
    - Supports operational reporting
    - Frequent updates (near real-time)
    - Short retention

    Data Warehouse (Curated):
    - Historical data (years)
    - Dimensional model (star/snowflake)
    - Supports analytical reporting
    - Less frequent updates (batch)
    - Long retention
*/

-- =============================================================================
-- SECTION 5: GRANT PRIVILEGES
-- =============================================================================

-- Engineer role gets full access to staging
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;

-- Analysts can read staging for troubleshooting
GRANT SELECT ON ALL TABLES IN SCHEMA STAGING TO ROLE RETAIL_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA STAGING TO ROLE RETAIL_ANALYST;
