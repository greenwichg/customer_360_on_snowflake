/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - SNOWPIPE SETUP
================================================================================
Purpose: Configure Snowpipe for continuous/automated data ingestion
Concepts: Auto-ingest, SQS notifications, serverless loading

Interview Points:
- Snowpipe is serverless, event-driven data loading
- Uses SQS queue for S3 event notifications
- Charges per file loaded (not per credit)
- Near real-time ingestion (1-2 minute latency)
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA LANDING;
USE WAREHOUSE LOADING_WH;

-- =============================================================================
-- SECTION 1: CREATE RAW LANDING TABLES FOR SNOWPIPE
-- =============================================================================
-- Snowpipe loads into these raw tables, then streams process into staging

-- -----------------------------------------------------------------------------
-- 1.1 Raw Sales Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_SALES (
    order_id VARCHAR,
    order_line_id VARCHAR,
    customer_id VARCHAR,
    product_id VARCHAR,
    store_id VARCHAR,
    transaction_date VARCHAR,
    quantity VARCHAR,
    unit_price VARCHAR,
    discount_percent VARCHAR,
    total_amount VARCHAR,
    payment_method VARCHAR,
    order_status VARCHAR,
    -- Metadata columns (populated by Snowpipe)
    _source_file VARCHAR,
    _file_row_number INTEGER,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE RAW_SALES IS 'Raw sales data loaded via Snowpipe - all VARCHAR for flexibility';

-- -----------------------------------------------------------------------------
-- 1.2 Raw Products Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_PRODUCTS (
    raw_json VARIANT,  -- Store entire JSON document
    -- Metadata columns
    _source_file VARCHAR,
    _file_row_number INTEGER,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE RAW_PRODUCTS IS 'Raw product data loaded via Snowpipe - JSON as VARIANT';

-- -----------------------------------------------------------------------------
-- 1.3 Raw Customers Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    date_of_birth VARCHAR,
    gender VARCHAR,
    registration_date VARCHAR,
    customer_segment VARCHAR,
    address_line1 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    loyalty_points VARCHAR,
    preferred_contact VARCHAR,
    is_active VARCHAR,
    -- Metadata columns
    _source_file VARCHAR,
    _file_row_number INTEGER,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE RAW_CUSTOMERS IS 'Raw customer data loaded via Snowpipe';

-- -----------------------------------------------------------------------------
-- 1.4 Raw Clickstream Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_CLICKSTREAM (
    raw_json VARIANT,  -- Store entire JSON event
    -- Metadata columns
    _source_file VARCHAR,
    _file_row_number INTEGER,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE RAW_CLICKSTREAM IS 'Raw clickstream events loaded via Snowpipe - JSON as VARIANT';

-- =============================================================================
-- SECTION 2: CREATE SNOWPIPES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 Snowpipe for Sales Data (CSV)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PIPE PIPE_SALES
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:YOUR_ACCOUNT:snowflake-retail-notifications'
    COMMENT = 'Snowpipe for auto-ingesting sales CSV files'
AS
COPY INTO RAW_SALES (
    order_id,
    order_line_id,
    customer_id,
    product_id,
    store_id,
    transaction_date,
    quantity,
    unit_price,
    discount_percent,
    total_amount,
    payment_method,
    order_status,
    _source_file,
    _file_row_number
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @INT_SALES_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD')
ON_ERROR = 'SKIP_FILE';  -- Skip problematic files, continue loading others

-- -----------------------------------------------------------------------------
-- 2.2 Snowpipe for Products Data (JSON)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PIPE PIPE_PRODUCTS
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:YOUR_ACCOUNT:snowflake-retail-notifications'
    COMMENT = 'Snowpipe for auto-ingesting product JSON files'
AS
COPY INTO RAW_PRODUCTS (raw_json, _source_file, _file_row_number)
FROM (
    SELECT
        $1::VARIANT,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @INT_PRODUCTS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'JSON_STANDARD')
ON_ERROR = 'SKIP_FILE';

-- -----------------------------------------------------------------------------
-- 2.3 Snowpipe for Customers Data (CSV)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PIPE PIPE_CUSTOMERS
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:YOUR_ACCOUNT:snowflake-retail-notifications'
    COMMENT = 'Snowpipe for auto-ingesting customer CSV files'
AS
COPY INTO RAW_CUSTOMERS (
    customer_id, first_name, last_name, email, phone,
    date_of_birth, gender, registration_date, customer_segment,
    address_line1, city, state, postal_code, country,
    loyalty_points, preferred_contact, is_active,
    _source_file, _file_row_number
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @INT_CUSTOMERS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD')
ON_ERROR = 'SKIP_FILE';

-- -----------------------------------------------------------------------------
-- 2.4 Snowpipe for Clickstream Data (JSON)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PIPE PIPE_CLICKSTREAM
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:YOUR_ACCOUNT:snowflake-retail-notifications'
    COMMENT = 'Snowpipe for auto-ingesting clickstream JSON events'
AS
COPY INTO RAW_CLICKSTREAM (raw_json, _source_file, _file_row_number)
FROM (
    SELECT
        $1::VARIANT,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @INT_CLICKSTREAM_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'JSON_STANDARD')
ON_ERROR = 'SKIP_FILE';

-- =============================================================================
-- SECTION 3: GET SNOWPIPE NOTIFICATION CHANNEL
-- =============================================================================
-- This ARN is needed to configure S3 event notifications

-- Get the SQS queue ARN for each pipe
SHOW PIPES;

-- Detailed pipe information
DESC PIPE PIPE_SALES;
DESC PIPE PIPE_CLICKSTREAM;

/*
The output includes:
- notification_channel: arn:aws:sqs:region:account:sf-snowpipe-xxxxx

Use this SQS ARN to:
1. Subscribe SQS queue to your SNS topic, OR
2. Configure S3 bucket notifications to send directly to SQS
*/

-- =============================================================================
-- SECTION 4: MONITOR SNOWPIPE STATUS
-- =============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('PIPE_SALES');
SELECT SYSTEM$PIPE_STATUS('PIPE_CLICKSTREAM');

-- Check recent load history for a pipe
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_SALES',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- Check pipe execution status
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
    DATE_RANGE_END => CURRENT_DATE(),
    PIPE_NAME => 'PIPE_SALES'
));

-- =============================================================================
-- SECTION 5: MANUAL SNOWPIPE OPERATIONS
-- =============================================================================

-- Manually trigger pipe to load new files
-- ALTER PIPE PIPE_SALES REFRESH;

-- Pause a pipe
-- ALTER PIPE PIPE_SALES SET PIPE_EXECUTION_PAUSED = TRUE;

-- Resume a pipe
-- ALTER PIPE PIPE_SALES SET PIPE_EXECUTION_PAUSED = FALSE;

-- Force refresh (scan for new files)
-- ALTER PIPE PIPE_SALES REFRESH;

-- =============================================================================
-- SECTION 6: ALTERNATIVE - REST API SNOWPIPE (No AWS Integration)
-- =============================================================================
/*
For environments without S3 events, use REST API to trigger Snowpipe:

1. Create pipe WITHOUT AUTO_INGEST
2. Call Snowpipe REST API when files are ready

Python example:
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile

ingest_manager = SimpleIngestManager(
    account='your_account',
    host='your_account.snowflakecomputing.com',
    user='your_user',
    pipe='RETAIL_ANALYTICS_DB.LANDING.PIPE_SALES',
    private_key_path='/path/to/rsa_key.p8'
)

staged_files = [StagedFile('sales_2024_01_15.csv', None)]
response = ingest_manager.ingest_files(staged_files)
*/

-- Non-auto-ingest pipe (for REST API triggering)
CREATE OR REPLACE PIPE PIPE_SALES_MANUAL
    AUTO_INGEST = FALSE
    COMMENT = 'Snowpipe triggered via REST API'
AS
COPY INTO RAW_SALES (
    order_id, order_line_id, customer_id, product_id, store_id,
    transaction_date, quantity, unit_price, discount_percent, total_amount,
    payment_method, order_status, _source_file, _file_row_number
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @INT_SALES_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD')
ON_ERROR = 'SKIP_FILE';

-- =============================================================================
-- SECTION 7: SNOWPIPE COST MONITORING
-- =============================================================================

-- Snowpipe credit usage (last 30 days)
/*
SELECT
    PIPE_NAME,
    DATE_TRUNC('day', START_TIME) AS usage_date,
    SUM(CREDITS_USED) AS daily_credits,
    SUM(FILES_INSERTED) AS files_loaded,
    SUM(BYTES_INSERTED) AS bytes_loaded
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, 2;
*/

-- Compare Snowpipe cost vs warehouse loading
/*
Snowpipe pricing: ~0.06 credits per 1000 files
Warehouse loading: Credit/hour based on size

Break-even analysis:
- For continuous small files: Snowpipe is cheaper
- For large batch files: Warehouse may be cheaper
- Consider: Latency requirements, file frequency, file size
*/

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: How does Snowpipe auto-ingest work?
A1:
    1. Files land in S3 bucket
    2. S3 sends ObjectCreated event to SNS
    3. SNS delivers notification to Snowpipe's SQS queue
    4. Snowpipe service detects new file notification
    5. Serverless compute loads the file
    6. Data appears in target table (1-2 min latency)

Q2: What's the difference between Snowpipe and COPY INTO?
A2:
    SNOWPIPE:
    - Serverless, event-driven
    - Near real-time (1-2 minutes)
    - Charged per file (not per credit-hour)
    - Auto-triggered by S3 events
    - Good for continuous small files

    COPY INTO:
    - Uses virtual warehouse
    - Batch loading (scheduled or manual)
    - Charged per credit-hour
    - You control when it runs
    - Good for large batch files

Q3: How do you handle Snowpipe errors?
A3:
    1. Check COPY_HISTORY for failed files:
       SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(...))
       WHERE STATUS = 'LOAD_FAILED';

    2. Check error message in FIRST_ERROR_MESSAGE column

    3. Common issues:
       - Schema mismatch
       - Invalid data format
       - Missing columns
       - Permission issues

    4. Reprocess failed files:
       - Fix the file
       - Remove from COPY tracking: REMOVE @stage/file.csv PURGE
       - ALTER PIPE REFRESH

Q4: How do you ensure exactly-once loading with Snowpipe?
A4:
    Snowpipe tracks loaded files via:
    - 14-day load history
    - File checksum/hash

    Same file won't reload unless:
    - File content changes (different hash)
    - You manually remove from history

    Best practices:
    - Use unique file names (include timestamp)
    - Don't modify files after upload
    - Monitor COPY_HISTORY for duplicates

Q5: When should you NOT use Snowpipe?
A5:
    Avoid Snowpipe when:
    - Loading very large files (>100MB) - warehouse may be cheaper
    - Need complex transformations during load
    - Loading from non-S3 sources without API integration
    - Need immediate data availability (Snowpipe has latency)
    - Files arrive in large batches at predictable times

Q6: How do you optimize Snowpipe performance?
A6:
    1. Keep files small (10-100 MB ideal)
    2. Use efficient formats (Parquet > CSV)
    3. Partition data into separate paths
    4. Use appropriate ON_ERROR setting
    5. Monitor load latency via PIPE_USAGE_HISTORY
    6. Scale horizontally with multiple pipes for different paths
*/

-- =============================================================================
-- SECTION 8: VERIFY SNOWPIPE SETUP
-- =============================================================================

-- Show all pipes
SHOW PIPES IN SCHEMA LANDING;

-- Verify tables exist
SHOW TABLES LIKE 'RAW_%' IN SCHEMA LANDING;

-- Check if pipes are running
SELECT
    "name" AS pipe_name,
    "notification_channel" AS sqs_arn,
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- =============================================================================
-- SECTION 9: GRANT PRIVILEGES
-- =============================================================================

-- Grant pipe operations to engineer role
GRANT OWNERSHIP ON PIPE PIPE_SALES TO ROLE RETAIL_ENGINEER;
GRANT OWNERSHIP ON PIPE PIPE_PRODUCTS TO ROLE RETAIL_ENGINEER;
GRANT OWNERSHIP ON PIPE PIPE_CUSTOMERS TO ROLE RETAIL_ENGINEER;
GRANT OWNERSHIP ON PIPE PIPE_CLICKSTREAM TO ROLE RETAIL_ENGINEER;
GRANT OWNERSHIP ON PIPE PIPE_SALES_MANUAL TO ROLE RETAIL_ENGINEER;

-- Grant table access
GRANT ALL PRIVILEGES ON TABLE RAW_SALES TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON TABLE RAW_PRODUCTS TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON TABLE RAW_CUSTOMERS TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON TABLE RAW_CLICKSTREAM TO ROLE RETAIL_ENGINEER;
