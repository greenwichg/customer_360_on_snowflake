/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - EXTERNAL STAGES
================================================================================
Purpose: Create external stages pointing to S3 data locations
Concepts: Stage creation, directory tables, stage metadata

Interview Points:
- External stages are pointers to cloud storage locations
- They don't store data, just reference it
- Can be used with COPY INTO or External Tables
- Directory tables enable efficient file management
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA LANDING;
USE WAREHOUSE LOADING_WH;

-- =============================================================================
-- SECTION 1: EXTERNAL STAGES WITH STORAGE INTEGRATION
-- =============================================================================
-- Note: These require the storage integration from setup/05_aws_integration.sql

-- -----------------------------------------------------------------------------
-- 1.1 Sales Data Stage
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE EXT_SALES_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/sales/'
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    FILE_FORMAT = CSV_STANDARD
    COMMENT = 'External stage for sales transaction data (CSV)';

-- -----------------------------------------------------------------------------
-- 1.2 Products Data Stage
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE EXT_PRODUCTS_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/products/'
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    FILE_FORMAT = JSON_STANDARD
    COMMENT = 'External stage for product catalog data (JSON)';

-- -----------------------------------------------------------------------------
-- 1.3 Customers Data Stage
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE EXT_CUSTOMERS_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/customers/'
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    FILE_FORMAT = PARQUET_STANDARD
    COMMENT = 'External stage for customer data (Parquet)';

-- -----------------------------------------------------------------------------
-- 1.4 Clickstream Data Stage (for Snowpipe)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE EXT_CLICKSTREAM_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/clickstream/'
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    FILE_FORMAT = JSON_STANDARD
    COMMENT = 'External stage for clickstream events (JSON, Snowpipe)';

-- -----------------------------------------------------------------------------
-- 1.5 Inventory Data Stage
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE EXT_INVENTORY_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/inventory/'
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    FILE_FORMAT = CSV_STANDARD
    COMMENT = 'External stage for inventory data (CSV)';

-- =============================================================================
-- SECTION 2: INTERNAL STAGES (For Local Testing)
-- =============================================================================
-- Use these stages when AWS is not configured

-- -----------------------------------------------------------------------------
-- 2.1 Internal Stage for Sales
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE INT_SALES_STAGE
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = CSV_STANDARD
    COMMENT = 'Internal stage for sales data (local testing)';

-- -----------------------------------------------------------------------------
-- 2.2 Internal Stage for Products
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE INT_PRODUCTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = JSON_STANDARD
    COMMENT = 'Internal stage for products data (local testing)';

-- -----------------------------------------------------------------------------
-- 2.3 Internal Stage for Customers
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE INT_CUSTOMERS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = CSV_STANDARD
    COMMENT = 'Internal stage for customers data (local testing)';

-- -----------------------------------------------------------------------------
-- 2.4 Internal Stage for Clickstream
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE INT_CLICKSTREAM_STAGE
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = JSON_STANDARD
    COMMENT = 'Internal stage for clickstream data (local testing)';

-- =============================================================================
-- SECTION 3: DIRECTORY TABLES
-- =============================================================================
/*
Directory tables provide metadata about staged files:
- File paths, sizes, last modified dates
- Enable efficient file management and incremental loading
- Auto-refresh keeps metadata in sync with S3
*/

-- Refresh directory table manually (if needed)
-- ALTER STAGE EXT_SALES_STAGE REFRESH;

-- Query directory table to see files
-- SELECT * FROM DIRECTORY(@EXT_SALES_STAGE);

-- Find files modified in last 24 hours
-- SELECT * FROM DIRECTORY(@EXT_SALES_STAGE)
-- WHERE LAST_MODIFIED > DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- =============================================================================
-- SECTION 4: STAGE OPERATIONS EXAMPLES
-- =============================================================================

-- List files in a stage
-- LIST @EXT_SALES_STAGE;
-- LIST @INT_SALES_STAGE;

-- Preview data from staged file (without loading)
-- SELECT
--     $1 AS col1,
--     $2 AS col2,
--     $3 AS col3,
--     METADATA$FILENAME AS filename,
--     METADATA$FILE_ROW_NUMBER AS row_num
-- FROM @INT_SALES_STAGE/sales_transactions.csv
-- (FILE_FORMAT => 'CSV_STANDARD')
-- LIMIT 10;

-- Get file metadata
-- SELECT
--     METADATA$FILENAME AS file_name,
--     METADATA$FILE_ROW_NUMBER AS row_number,
--     METADATA$FILE_CONTENT_KEY AS content_key,
--     METADATA$FILE_LAST_MODIFIED AS last_modified,
--     METADATA$START_SCAN_TIME AS scan_time
-- FROM @INT_SALES_STAGE
-- LIMIT 5;

-- =============================================================================
-- SECTION 5: UPLOAD FILES TO INTERNAL STAGE (Using SnowSQL)
-- =============================================================================
/*
Use SnowSQL CLI to upload local files to internal stages:

# Connect to Snowflake
snowsql -a <account> -u <user>

# Use correct context
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA LANDING;

# Upload files (from local machine)
PUT file:///path/to/sales_transactions.csv @INT_SALES_STAGE AUTO_COMPRESS=TRUE;
PUT file:///path/to/products.json @INT_PRODUCTS_STAGE AUTO_COMPRESS=TRUE;
PUT file:///path/to/customers.csv @INT_CUSTOMERS_STAGE AUTO_COMPRESS=TRUE;
PUT file:///path/to/clickstream_events.json @INT_CLICKSTREAM_STAGE AUTO_COMPRESS=TRUE;

# Verify uploads
LIST @INT_SALES_STAGE;
LIST @INT_PRODUCTS_STAGE;

# PUT options:
# AUTO_COMPRESS=TRUE  - Compress files automatically (gzip)
# PARALLEL=4          - Number of parallel upload threads
# OVERWRITE=TRUE      - Overwrite existing files
*/

-- =============================================================================
-- SECTION 6: VERIFY STAGE SETUP
-- =============================================================================

-- Show all stages
SHOW STAGES IN SCHEMA LANDING;

-- Get stage details
DESC STAGE INT_SALES_STAGE;

-- Show storage integration (requires ACCOUNTADMIN)
-- SHOW STORAGE INTEGRATIONS;

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: What's the difference between External and Internal stages?
A1:
    EXTERNAL STAGE:
    - Points to cloud storage (S3, Azure Blob, GCS)
    - Data stays in your cloud account
    - Uses storage integration for authentication
    - Can share data without moving it
    - Pay for storage in your cloud account

    INTERNAL STAGE:
    - Data stored in Snowflake's managed storage
    - Three types: User, Table, Named
    - Included in Snowflake storage costs
    - Simpler setup (no cloud integration)
    - Good for small files or temp uploads

Q2: What is a directory table and when should you use it?
A2:
    Directory tables store metadata about staged files:
    - File path, size, last modified, checksum
    - Enables efficient incremental loading
    - Auto-refresh keeps metadata current

    Use cases:
    - Track which files have been loaded
    - Find new/modified files for incremental loads
    - Audit file landing times
    - Build external tables on staged data

Q3: How do you handle large file uploads efficiently?
A3:
    1. Use PARALLEL option in PUT command (parallel threads)
    2. Split large files into smaller chunks (100-250 MB ideal)
    3. Compress files (AUTO_COMPRESS=TRUE)
    4. Use dedicated loading warehouse
    5. Consider Snowpipe for continuous loading

Q4: What's the difference between LIST and DIRECTORY?
A4:
    LIST @stage:
    - Returns files directly from cloud storage
    - Always up-to-date but slower
    - No filtering/sorting capabilities
    - Simple output (name, size, date)

    DIRECTORY(@stage):
    - Queries cached metadata table
    - Faster for large file counts
    - Full SQL filtering/sorting
    - Rich metadata (checksums, etc.)
    - Requires DIRECTORY = (ENABLE = TRUE)

Q5: How do you implement incremental loading using stages?
A5:
    Using COPY command with metadata:
    1. Track loaded files in a control table
    2. Query directory table for new files
    3. COPY only unprocessed files using PATTERN or FILES

    Example pattern:
    - Load files with METADATA$FILENAME
    - Store filename + load_timestamp in tracking table
    - Next load: exclude already-processed files

    Or use Snowpipe for automatic incremental loading.
*/

-- =============================================================================
-- SECTION 7: GRANT PRIVILEGES
-- =============================================================================

-- Grant stage usage to roles
GRANT USAGE ON STAGE INT_SALES_STAGE TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE INT_PRODUCTS_STAGE TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE INT_CUSTOMERS_STAGE TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE INT_CLICKSTREAM_STAGE TO ROLE RETAIL_ENGINEER;

-- Read and write for internal stages
GRANT READ, WRITE ON STAGE INT_SALES_STAGE TO ROLE RETAIL_ENGINEER;
GRANT READ, WRITE ON STAGE INT_PRODUCTS_STAGE TO ROLE RETAIL_ENGINEER;
GRANT READ, WRITE ON STAGE INT_CUSTOMERS_STAGE TO ROLE RETAIL_ENGINEER;
GRANT READ, WRITE ON STAGE INT_CLICKSTREAM_STAGE TO ROLE RETAIL_ENGINEER;
