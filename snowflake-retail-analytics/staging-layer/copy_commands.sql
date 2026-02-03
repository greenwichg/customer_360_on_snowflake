/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - COPY COMMANDS
================================================================================
Purpose: Load data from stages into staging tables with transformations
Concepts: COPY INTO, error handling, transformations, metadata

Interview Points:
- COPY INTO is the primary bulk loading mechanism
- Supports transformations during load
- Various error handling options
- Metadata columns capture file info
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA STAGING;
USE WAREHOUSE LOADING_WH;

-- =============================================================================
-- SECTION 1: COPY SALES DATA (CSV)
-- =============================================================================

-- Full load from stage (use for initial load or full refresh)
COPY INTO STG_SALES (
    order_id, order_line_id, customer_id, product_id, store_id,
    transaction_date, quantity, unit_price, discount_percent, total_amount,
    payment_method, order_status, source_file, file_row_number
)
FROM (
    SELECT
        $1,                                      -- order_id
        TRY_TO_NUMBER($2),                       -- order_line_id
        $3,                                      -- customer_id
        $4,                                      -- product_id
        $5,                                      -- store_id
        TRY_TO_TIMESTAMP($6),                    -- transaction_date
        TRY_TO_NUMBER($7),                       -- quantity
        TRY_TO_DECIMAL($8, 10, 2),               -- unit_price
        TRY_TO_DECIMAL($9, 5, 2),                -- discount_percent
        TRY_TO_DECIMAL($10, 12, 2),              -- total_amount
        UPPER(TRIM($11)),                        -- payment_method (standardized)
        UPPER(TRIM($12)),                        -- order_status (standardized)
        METADATA$FILENAME,                       -- source_file
        METADATA$FILE_ROW_NUMBER                 -- file_row_number
    FROM @LANDING.INT_SALES_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_STANDARD')
PATTERN = '.*[.]csv'
ON_ERROR = 'CONTINUE'                             -- Continue on row errors
FORCE = FALSE                                     -- Skip already loaded files
PURGE = FALSE;                                    -- Keep files after load

-- =============================================================================
-- SECTION 2: COPY PRODUCTS DATA (JSON)
-- =============================================================================

COPY INTO STG_PRODUCTS (
    product_id, product_name, category, subcategory, brand,
    unit_cost, unit_price, weight_kg, is_active, launch_date,
    attributes, source_file, file_row_number
)
FROM (
    SELECT
        $1:product_id::VARCHAR,
        $1:product_name::VARCHAR,
        $1:category::VARCHAR,
        $1:subcategory::VARCHAR,
        $1:brand::VARCHAR,
        $1:unit_cost::DECIMAL(10,2),
        $1:unit_price::DECIMAL(10,2),
        $1:weight_kg::DECIMAL(8,3),
        $1:is_active::BOOLEAN,
        TRY_TO_DATE($1:launch_date::VARCHAR),
        $1:attributes::VARIANT,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @LANDING.INT_PRODUCTS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.JSON_STANDARD')
ON_ERROR = 'CONTINUE';

-- =============================================================================
-- SECTION 3: COPY CUSTOMERS DATA (CSV)
-- =============================================================================

COPY INTO STG_CUSTOMERS (
    customer_id, first_name, last_name, email, phone,
    date_of_birth, gender, registration_date, customer_segment,
    address_line1, city, state, postal_code, country,
    loyalty_points, preferred_contact, is_active,
    source_file, file_row_number, record_hash
)
FROM (
    SELECT
        $1,                                      -- customer_id
        INITCAP(TRIM($2)),                       -- first_name (standardized)
        INITCAP(TRIM($3)),                       -- last_name
        LOWER(TRIM($4)),                         -- email (lowercase)
        $5,                                      -- phone
        TRY_TO_DATE($6),                         -- date_of_birth
        UPPER(TRIM($7)),                         -- gender
        TRY_TO_DATE($8),                         -- registration_date
        UPPER(TRIM($9)),                         -- customer_segment
        $10,                                     -- address_line1
        INITCAP(TRIM($11)),                      -- city
        UPPER(TRIM($12)),                        -- state
        $13,                                     -- postal_code
        UPPER(TRIM($14)),                        -- country
        TRY_TO_NUMBER($15),                      -- loyalty_points
        UPPER(TRIM($16)),                        -- preferred_contact
        TRY_TO_BOOLEAN($17),                     -- is_active
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        -- Hash for SCD change detection (exclude audit columns)
        MD5(CONCAT_WS('|',
            COALESCE($2,''), COALESCE($3,''), COALESCE($4,''),
            COALESCE($5,''), COALESCE($9,''), COALESCE($10,''),
            COALESCE($11,''), COALESCE($12,''), COALESCE($13,''),
            COALESCE($15,'')
        ))
    FROM @LANDING.INT_CUSTOMERS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_STANDARD')
ON_ERROR = 'CONTINUE';

-- =============================================================================
-- SECTION 4: COPY CLICKSTREAM DATA (JSON)
-- =============================================================================

COPY INTO STG_CLICKSTREAM (
    event_id, session_id, customer_id, event_timestamp, event_type,
    page_url, referrer_url, device_type, browser, os, ip_address,
    duration_seconds, scroll_depth_percent, product_id, quantity,
    order_id, order_value, cart_value, search_query,
    source_file, file_row_number
)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:session_id::VARCHAR,
        $1:customer_id::VARCHAR,
        TRY_TO_TIMESTAMP($1:event_timestamp::VARCHAR),
        UPPER($1:event_type::VARCHAR),
        $1:page_url::VARCHAR,
        $1:referrer_url::VARCHAR,
        LOWER($1:device_type::VARCHAR),
        $1:browser::VARCHAR,
        $1:os::VARCHAR,
        $1:ip_address::VARCHAR,
        $1:duration_seconds::INTEGER,
        $1:scroll_depth_percent::INTEGER,
        $1:product_id::VARCHAR,
        $1:quantity::INTEGER,
        $1:order_id::VARCHAR,
        $1:order_value::DECIMAL(12,2),
        $1:cart_value::DECIMAL(12,2),
        $1:search_query::VARCHAR,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @LANDING.INT_CLICKSTREAM_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.JSON_STANDARD')
ON_ERROR = 'CONTINUE';

-- =============================================================================
-- SECTION 5: INCREMENTAL LOADING WITH FILE TRACKING
-- =============================================================================

-- Procedure for incremental loading (loads only new files)
CREATE OR REPLACE PROCEDURE SP_INCREMENTAL_LOAD_SALES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    files_loaded INTEGER;
    rows_loaded INTEGER;
BEGIN
    -- Get list of unprocessed files
    CREATE OR REPLACE TEMPORARY TABLE TEMP_NEW_FILES AS
    SELECT DISTINCT METADATA$FILENAME AS file_name
    FROM @LANDING.INT_SALES_STAGE
    WHERE METADATA$FILENAME NOT IN (
        SELECT file_path FROM STG_FILE_REGISTRY
        WHERE process_status = 'LOADED'
    );

    -- Load new files only
    COPY INTO STG_SALES (
        order_id, order_line_id, customer_id, product_id, store_id,
        transaction_date, quantity, unit_price, discount_percent, total_amount,
        payment_method, order_status, source_file, file_row_number
    )
    FROM (
        SELECT $1, TRY_TO_NUMBER($2), $3, $4, $5, TRY_TO_TIMESTAMP($6),
               TRY_TO_NUMBER($7), TRY_TO_DECIMAL($8, 10, 2),
               TRY_TO_DECIMAL($9, 5, 2), TRY_TO_DECIMAL($10, 12, 2),
               UPPER(TRIM($11)), UPPER(TRIM($12)),
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
        FROM @LANDING.INT_SALES_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_STANDARD')
    FILES = (SELECT file_name FROM TEMP_NEW_FILES)
    ON_ERROR = 'CONTINUE';

    -- Record loaded files
    INSERT INTO STG_FILE_REGISTRY (file_path, process_status, process_timestamp)
    SELECT file_name, 'LOADED', CURRENT_TIMESTAMP()
    FROM TEMP_NEW_FILES;

    RETURN 'Incremental load completed';
END;
$$;

-- =============================================================================
-- SECTION 6: ERROR HANDLING OPTIONS
-- =============================================================================
/*
ON_ERROR options:
- CONTINUE: Skip error rows, continue loading
- SKIP_FILE: Skip entire file on error
- SKIP_FILE_n: Skip file after n errors (e.g., SKIP_FILE_10)
- SKIP_FILE_n%: Skip file after n% errors
- ABORT_STATEMENT: Stop entire COPY operation

VALIDATION_MODE options (test without loading):
- RETURN_n_ROWS: Return first n rows that would be loaded
- RETURN_ERRORS: Return all rows that would fail
- RETURN_ALL_ERRORS: Return all errors (may be slow)
*/

-- Validate files before loading (dry run)
-- COPY INTO STG_SALES (...)
-- FROM @LANDING.INT_SALES_STAGE
-- FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_STANDARD')
-- VALIDATION_MODE = 'RETURN_ERRORS';

-- =============================================================================
-- SECTION 7: CHECK COPY HISTORY
-- =============================================================================

-- Recent copy operations
SELECT
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    STATUS,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STG_SALES',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: What's the difference between COPY and INSERT INTO SELECT?
A1:
    COPY INTO:
    - Optimized for bulk loading from files
    - Tracks loaded files (won't reload same file)
    - Supports various file formats
    - Can transform during load

    INSERT INTO SELECT:
    - General SQL INSERT
    - No file tracking
    - Works with any SELECT
    - No file format support

Q2: How do you handle incremental loading?
A2:
    Option 1: COPY default behavior (tracks loaded files)
    Option 2: Custom file registry table (more control)
    Option 3: Use Snowpipe (event-driven)
    Option 4: Filter by METADATA$FILE_LAST_MODIFIED

Q3: What happens if COPY fails midway?
A3:
    - Depends on ON_ERROR setting
    - Successfully loaded data is committed
    - Failed rows/files are reported
    - Use VALIDATION_MODE to test before loading
*/
