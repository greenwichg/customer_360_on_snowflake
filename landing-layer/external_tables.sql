/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - EXTERNAL TABLES
================================================================================
Purpose: Create external tables to query data directly in S3 without loading
Concepts: Schema-on-read, partition columns, auto-refresh

Interview Points:
- External tables allow querying cloud storage without loading
- Schema-on-read: define schema at query time
- Partitions improve query performance (pruning)
- Good for data lake patterns or rarely-accessed data
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA LANDING;
USE WAREHOUSE LOADING_WH;

-- =============================================================================
-- SECTION 1: EXTERNAL TABLE FOR SALES DATA (CSV)
-- =============================================================================
/*
External table for sales data with partition columns derived from file path.
Pattern: s3://bucket/landing/sales/year=2024/month=01/sales_data.csv
*/

CREATE OR REPLACE EXTERNAL TABLE EXT_TBL_SALES
    WITH LOCATION = @EXT_SALES_STAGE
    AUTO_REFRESH = TRUE
    PARTITION BY (partition_year, partition_month)
    FILE_FORMAT = CSV_STANDARD
    PATTERN = '.*[.]csv'
AS
SELECT
    -- Data columns (parsed from file)
    VALUE:c1::VARCHAR AS order_id,
    VALUE:c2::INTEGER AS order_line_id,
    VALUE:c3::VARCHAR AS customer_id,
    VALUE:c4::VARCHAR AS product_id,
    VALUE:c5::VARCHAR AS store_id,
    TRY_TO_TIMESTAMP(VALUE:c6::VARCHAR) AS transaction_date,
    VALUE:c7::INTEGER AS quantity,
    VALUE:c8::FLOAT AS unit_price,
    VALUE:c9::FLOAT AS discount_percent,
    VALUE:c10::FLOAT AS total_amount,
    VALUE:c11::VARCHAR AS payment_method,
    VALUE:c12::VARCHAR AS order_status,
    -- Partition columns (derived from path)
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 4), '=', 2)::INTEGER AS partition_year,
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 5), '=', 2)::INTEGER AS partition_month,
    -- Metadata columns
    METADATA$FILENAME::VARCHAR AS source_file,
    METADATA$FILE_ROW_NUMBER::INTEGER AS file_row_number,
    METADATA$FILE_LAST_MODIFIED::TIMESTAMP_NTZ AS file_last_modified;

-- Add comment
COMMENT ON TABLE EXT_TBL_SALES IS 'External table for sales data - queries S3 directly';

-- =============================================================================
-- SECTION 2: EXTERNAL TABLE FOR PRODUCTS DATA (JSON)
-- =============================================================================
/*
External table for JSON product data.
Each row in the external table represents one JSON object.
*/

CREATE OR REPLACE EXTERNAL TABLE EXT_TBL_PRODUCTS
    WITH LOCATION = @EXT_PRODUCTS_STAGE
    AUTO_REFRESH = TRUE
    FILE_FORMAT = JSON_STANDARD
    PATTERN = '.*[.]json'
AS
SELECT
    -- Parse JSON fields
    VALUE:product_id::VARCHAR AS product_id,
    VALUE:product_name::VARCHAR AS product_name,
    VALUE:category::VARCHAR AS category,
    VALUE:subcategory::VARCHAR AS subcategory,
    VALUE:brand::VARCHAR AS brand,
    VALUE:unit_cost::FLOAT AS unit_cost,
    VALUE:unit_price::FLOAT AS unit_price,
    VALUE:weight_kg::FLOAT AS weight_kg,
    VALUE:is_active::BOOLEAN AS is_active,
    TRY_TO_DATE(VALUE:launch_date::VARCHAR) AS launch_date,
    VALUE:attributes::VARIANT AS attributes,  -- Keep nested JSON as VARIANT
    -- Metadata
    METADATA$FILENAME::VARCHAR AS source_file,
    METADATA$FILE_ROW_NUMBER::INTEGER AS file_row_number;

COMMENT ON TABLE EXT_TBL_PRODUCTS IS 'External table for product catalog (JSON)';

-- =============================================================================
-- SECTION 3: EXTERNAL TABLE FOR CUSTOMERS DATA (PARQUET)
-- =============================================================================
/*
External table for Parquet customer data.
Parquet schema is auto-inferred, but we define explicit columns for control.
*/

CREATE OR REPLACE EXTERNAL TABLE EXT_TBL_CUSTOMERS
    WITH LOCATION = @EXT_CUSTOMERS_STAGE
    AUTO_REFRESH = TRUE
    FILE_FORMAT = PARQUET_STANDARD
    PATTERN = '.*[.]parquet'
AS
SELECT
    VALUE:customer_id::VARCHAR AS customer_id,
    VALUE:first_name::VARCHAR AS first_name,
    VALUE:last_name::VARCHAR AS last_name,
    VALUE:email::VARCHAR AS email,
    VALUE:phone::VARCHAR AS phone,
    VALUE:date_of_birth::DATE AS date_of_birth,
    VALUE:gender::VARCHAR AS gender,
    VALUE:registration_date::DATE AS registration_date,
    VALUE:customer_segment::VARCHAR AS customer_segment,
    VALUE:address_line1::VARCHAR AS address_line1,
    VALUE:city::VARCHAR AS city,
    VALUE:state::VARCHAR AS state,
    VALUE:postal_code::VARCHAR AS postal_code,
    VALUE:country::VARCHAR AS country,
    VALUE:loyalty_points::INTEGER AS loyalty_points,
    VALUE:preferred_contact::VARCHAR AS preferred_contact,
    VALUE:is_active::BOOLEAN AS is_active,
    METADATA$FILENAME::VARCHAR AS source_file;

COMMENT ON TABLE EXT_TBL_CUSTOMERS IS 'External table for customer data (Parquet)';

-- =============================================================================
-- SECTION 4: EXTERNAL TABLE FOR CLICKSTREAM DATA (JSON)
-- =============================================================================
/*
External table for clickstream events with time-based partitioning.
Pattern: s3://bucket/landing/clickstream/year=2024/month=01/day=15/events.json
*/

CREATE OR REPLACE EXTERNAL TABLE EXT_TBL_CLICKSTREAM
    WITH LOCATION = @EXT_CLICKSTREAM_STAGE
    AUTO_REFRESH = TRUE
    PARTITION BY (event_date)
    FILE_FORMAT = JSON_STANDARD
    PATTERN = '.*[.]json'
AS
SELECT
    VALUE:event_id::VARCHAR AS event_id,
    VALUE:customer_id::VARCHAR AS customer_id,
    VALUE:session_id::VARCHAR AS session_id,
    TRY_TO_TIMESTAMP(VALUE:event_timestamp::VARCHAR) AS event_timestamp,
    VALUE:event_type::VARCHAR AS event_type,
    VALUE:page_url::VARCHAR AS page_url,
    VALUE:referrer_url::VARCHAR AS referrer_url,
    VALUE:device_type::VARCHAR AS device_type,
    VALUE:browser::VARCHAR AS browser,
    VALUE:os::VARCHAR AS os,
    VALUE:ip_address::VARCHAR AS ip_address,
    VALUE:duration_seconds::INTEGER AS duration_seconds,
    VALUE:scroll_depth_percent::INTEGER AS scroll_depth_percent,
    VALUE:product_id::VARCHAR AS product_id,
    VALUE:quantity::INTEGER AS quantity,
    VALUE:order_id::VARCHAR AS order_id,
    VALUE:order_value::FLOAT AS order_value,
    VALUE:search_query::VARCHAR AS search_query,
    VALUE:cart_value::FLOAT AS cart_value,
    -- Partition column
    TRY_TO_DATE(VALUE:event_timestamp::VARCHAR)::DATE AS event_date,
    METADATA$FILENAME::VARCHAR AS source_file,
    METADATA$FILE_ROW_NUMBER::INTEGER AS file_row_number;

COMMENT ON TABLE EXT_TBL_CLICKSTREAM IS 'External table for clickstream events (JSON)';

-- =============================================================================
-- SECTION 5: MANAGING EXTERNAL TABLES
-- =============================================================================

-- Refresh external table metadata manually
-- ALTER EXTERNAL TABLE EXT_TBL_SALES REFRESH;

-- Add partitions manually (if AUTO_REFRESH is false)
-- ALTER EXTERNAL TABLE EXT_TBL_SALES ADD PARTITION(partition_year='2024', partition_month='01')
--     LOCATION 'year=2024/month=01/';

-- Remove partition
-- ALTER EXTERNAL TABLE EXT_TBL_SALES DROP PARTITION LOCATION 'year=2023/month=01/';

-- =============================================================================
-- SECTION 6: QUERYING EXTERNAL TABLES
-- =============================================================================

-- Simple query (scans all files)
-- SELECT * FROM EXT_TBL_SALES LIMIT 100;

-- Query with partition pruning (much faster!)
-- SELECT * FROM EXT_TBL_SALES
-- WHERE partition_year = 2024 AND partition_month = 1;

-- Aggregate query
-- SELECT
--     partition_month,
--     COUNT(*) AS order_count,
--     SUM(total_amount) AS total_revenue
-- FROM EXT_TBL_SALES
-- WHERE partition_year = 2024
-- GROUP BY partition_month
-- ORDER BY partition_month;

-- Join external tables
-- SELECT
--     s.order_id,
--     c.first_name,
--     c.last_name,
--     p.product_name,
--     s.total_amount
-- FROM EXT_TBL_SALES s
-- LEFT JOIN EXT_TBL_CUSTOMERS c ON s.customer_id = c.customer_id
-- LEFT JOIN EXT_TBL_PRODUCTS p ON s.product_id = p.product_id
-- WHERE s.partition_year = 2024 AND s.partition_month = 1
-- LIMIT 100;

-- =============================================================================
-- SECTION 7: MATERIALIZED VIEW ON EXTERNAL TABLE
-- =============================================================================
/*
Create a materialized view on external table for frequently-accessed aggregations.
This caches the results and auto-refreshes.
*/

-- Note: Materialized views on external tables have some limitations
-- CREATE MATERIALIZED VIEW MV_EXTERNAL_SALES_SUMMARY AS
-- SELECT
--     partition_year,
--     partition_month,
--     COUNT(*) AS order_count,
--     SUM(total_amount) AS total_revenue,
--     AVG(total_amount) AS avg_order_value
-- FROM EXT_TBL_SALES
-- GROUP BY partition_year, partition_month;

-- =============================================================================
-- SECTION 8: VERIFY EXTERNAL TABLES
-- =============================================================================

-- Show external tables
SHOW EXTERNAL TABLES IN SCHEMA LANDING;

-- Describe external table structure
-- DESC EXTERNAL TABLE EXT_TBL_SALES;

-- Check table metadata
-- SELECT
--     TABLE_NAME,
--     TABLE_TYPE,
--     ROW_COUNT,
--     BYTES,
--     CREATED,
--     LAST_ALTERED
-- FROM INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_SCHEMA = 'LANDING'
-- AND TABLE_TYPE = 'EXTERNAL TABLE';

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: When should you use External Tables vs COPY INTO?
A1:
    USE EXTERNAL TABLES when:
    - Data is queried infrequently
    - You want to avoid data duplication
    - Data lake integration is needed
    - Storage costs need to stay in your cloud account
    - Schema exploration before committing to load

    USE COPY INTO when:
    - Data is queried frequently
    - Query performance is critical
    - You need full Snowflake features (Time Travel, etc.)
    - Data transformations are needed
    - Joining with other Snowflake tables regularly

Q2: How do partitions improve external table performance?
A2:
    Partitions enable PRUNING:
    - External table scans only relevant files
    - Partition columns derived from file path
    - WHERE clause on partition = fewer files scanned
    - Can reduce scan from 1000s of files to just a few

    Example: WHERE partition_year = 2024 AND partition_month = 1
    Only scans files in the /year=2024/month=01/ path

Q3: What's the difference between AUTO_REFRESH and manual refresh?
A3:
    AUTO_REFRESH = TRUE:
    - Snowflake monitors cloud storage for changes
    - Automatically refreshes metadata when files change
    - Small cost for the background service
    - Recommended for frequently updated data

    Manual REFRESH:
    - You run ALTER EXTERNAL TABLE ... REFRESH
    - Use for infrequently updated data
    - No background service cost
    - More control over refresh timing

Q4: What are the limitations of external tables?
A4:
    - No DML operations (INSERT/UPDATE/DELETE)
    - No Time Travel
    - No clustering keys
    - Slower than native tables
    - Limited join performance
    - Some functions may not work
    - Materialized views have restrictions

Q5: How do you handle schema changes in external tables?
A5:
    Options:
    1. Recreate external table with new schema
    2. Use VARIANT columns for flexibility
    3. Version the S3 path: /v1/, /v2/
    4. Add columns without removing (backward compatible)

    Best practice: Use VARIANT for evolving schemas,
    then extract columns as needed:
    VALUE:new_field::VARCHAR AS new_field

Q6: How do you optimize external table queries?
A6:
    1. Use partition columns in WHERE clauses
    2. Select only needed columns (not SELECT *)
    3. Push down predicates to storage layer
    4. Use appropriate file formats (Parquet > CSV)
    5. Keep file sizes between 100-250 MB
    6. Consider materializing frequently-accessed data
*/

-- =============================================================================
-- SECTION 9: GRANT PRIVILEGES
-- =============================================================================

-- Grant access to external tables
GRANT SELECT ON TABLE EXT_TBL_SALES TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON TABLE EXT_TBL_PRODUCTS TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON TABLE EXT_TBL_CUSTOMERS TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON TABLE EXT_TBL_CLICKSTREAM TO ROLE RETAIL_ENGINEER;

-- Analysts can query external tables (read-only)
GRANT SELECT ON TABLE EXT_TBL_SALES TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE EXT_TBL_PRODUCTS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE EXT_TBL_CUSTOMERS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE EXT_TBL_CLICKSTREAM TO ROLE RETAIL_ANALYST;
