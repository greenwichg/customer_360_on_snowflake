/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - STREAMS (CHANGE DATA CAPTURE)
================================================================================
Purpose: Create streams on staging tables to track changes for incremental processing
Concepts: Stream types, metadata columns, stream consumption

Interview Points:
- Streams track INSERT/UPDATE/DELETE on tables
- Used for incremental ETL (process only changed data)
- Stream data is consumed when used in DML (exactly-once)
- Three types: Standard, Append-only, Insert-only
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA STAGING;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: CREATE STREAMS ON STAGING TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 Stream on Staged Sales
-- -----------------------------------------------------------------------------
/*
Standard stream captures INSERT, UPDATE, DELETE
- Use for fact table processing where all changes matter
*/
CREATE OR REPLACE STREAM STG_SALES_STREAM
    ON TABLE STG_SALES
    APPEND_ONLY = FALSE      -- Track all DML (INSERT, UPDATE, DELETE)
    SHOW_INITIAL_ROWS = FALSE -- Don't show existing rows initially
    COMMENT = 'CDC stream for incremental sales fact loading';

-- -----------------------------------------------------------------------------
-- 1.2 Stream on Staged Customers (for SCD Type 2)
-- -----------------------------------------------------------------------------
/*
Standard stream to detect customer changes
- Compare record_hash to detect actual changes
- Used for SCD Type 2 dimension processing
*/
CREATE OR REPLACE STREAM STG_CUSTOMERS_STREAM
    ON TABLE STG_CUSTOMERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for customer dimension (SCD Type 2)';

-- -----------------------------------------------------------------------------
-- 1.3 Stream on Staged Products (for SCD Type 1)
-- -----------------------------------------------------------------------------
/*
Standard stream for product changes
- Products use SCD Type 1 (overwrite)
*/
CREATE OR REPLACE STREAM STG_PRODUCTS_STREAM
    ON TABLE STG_PRODUCTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for product dimension (SCD Type 1)';

-- -----------------------------------------------------------------------------
-- 1.4 Stream on Staged Clickstream (Append-Only)
-- -----------------------------------------------------------------------------
/*
Append-only stream for clickstream events
- Events are immutable (no updates/deletes)
- More efficient than standard stream
*/
CREATE OR REPLACE STREAM STG_CLICKSTREAM_STREAM
    ON TABLE STG_CLICKSTREAM
    APPEND_ONLY = TRUE       -- Only track INSERTs (more efficient)
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Append-only stream for clickstream fact loading';

-- -----------------------------------------------------------------------------
-- 1.5 Stream on Staged Stores
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STREAM STG_STORES_STREAM
    ON TABLE STG_STORES
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for store dimension';

-- =============================================================================
-- SECTION 2: STREAM METADATA COLUMNS
-- =============================================================================
/*
Streams add special metadata columns:
- METADATA$ACTION: 'INSERT', 'DELETE'
- METADATA$ISUPDATE: TRUE if this is part of an UPDATE
- METADATA$ROW_ID: Unique ID for the change

For UPDATES:
- Shows as DELETE (old row) + INSERT (new row)
- Both have METADATA$ISUPDATE = TRUE
- Can be combined to see before/after
*/

-- Example: View stream contents with metadata
-- SELECT
--     customer_id,
--     first_name,
--     last_name,
--     email,
--     METADATA$ACTION AS change_type,
--     METADATA$ISUPDATE AS is_update,
--     METADATA$ROW_ID AS row_id
-- FROM STG_CUSTOMERS_STREAM;

-- =============================================================================
-- SECTION 3: WORKING WITH STREAMS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 Check Stream Data Available
-- -----------------------------------------------------------------------------
-- Use SYSTEM$STREAM_HAS_DATA() before processing (avoids empty runs)

CREATE OR REPLACE PROCEDURE CHECK_STREAM_STATUS()
RETURNS TABLE (stream_name VARCHAR, has_data BOOLEAN)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    result := (
        SELECT 'STG_SALES_STREAM' AS stream_name,
               SYSTEM$STREAM_HAS_DATA('STG_SALES_STREAM') AS has_data
        UNION ALL
        SELECT 'STG_CUSTOMERS_STREAM',
               SYSTEM$STREAM_HAS_DATA('STG_CUSTOMERS_STREAM')
        UNION ALL
        SELECT 'STG_PRODUCTS_STREAM',
               SYSTEM$STREAM_HAS_DATA('STG_PRODUCTS_STREAM')
        UNION ALL
        SELECT 'STG_CLICKSTREAM_STREAM',
               SYSTEM$STREAM_HAS_DATA('STG_CLICKSTREAM_STREAM')
        UNION ALL
        SELECT 'STG_STORES_STREAM',
               SYSTEM$STREAM_HAS_DATA('STG_STORES_STREAM')
    );
    RETURN TABLE(result);
END;
$$;

-- Call: CALL CHECK_STREAM_STATUS();

-- -----------------------------------------------------------------------------
-- 3.2 Get Change Summary
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_STREAM_CHANGE_SUMMARY AS
SELECT
    'STG_SALES_STREAM' AS stream_name,
    COUNT(*) AS total_changes,
    SUM(CASE WHEN METADATA$ACTION = 'INSERT' AND NOT METADATA$ISUPDATE THEN 1 ELSE 0 END) AS inserts,
    SUM(CASE WHEN METADATA$ISUPDATE THEN 1 ELSE 0 END) / 2 AS updates,  -- Updates appear as 2 rows
    SUM(CASE WHEN METADATA$ACTION = 'DELETE' AND NOT METADATA$ISUPDATE THEN 1 ELSE 0 END) AS deletes
FROM STG_SALES_STREAM
UNION ALL
SELECT
    'STG_CUSTOMERS_STREAM',
    COUNT(*),
    SUM(CASE WHEN METADATA$ACTION = 'INSERT' AND NOT METADATA$ISUPDATE THEN 1 ELSE 0 END),
    SUM(CASE WHEN METADATA$ISUPDATE THEN 1 ELSE 0 END) / 2,
    SUM(CASE WHEN METADATA$ACTION = 'DELETE' AND NOT METADATA$ISUPDATE THEN 1 ELSE 0 END)
FROM STG_CUSTOMERS_STREAM;

-- =============================================================================
-- SECTION 4: STREAM CONSUMPTION PATTERNS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 Simple INSERT from Stream
-- -----------------------------------------------------------------------------
/*
When you SELECT from stream in a DML, the stream is "consumed"
- Consumed data won't appear in subsequent reads
- This is "exactly-once" processing guarantee
*/

-- Example: Insert new sales into fact table
-- INSERT INTO CURATED.FACT_SALES (...)
-- SELECT ... FROM STG_SALES_STREAM
-- WHERE METADATA$ACTION = 'INSERT';
-- After this runs, stream is empty for processed records

-- -----------------------------------------------------------------------------
-- 4.2 MERGE Pattern (Upsert)
-- -----------------------------------------------------------------------------
/*
Use MERGE for SCD Type 1 (overwrite latest)
*/

-- Example MERGE for products (SCD Type 1)
-- MERGE INTO CURATED.DIM_PRODUCT target
-- USING (
--     SELECT * FROM STG_PRODUCTS_STREAM
--     WHERE METADATA$ACTION = 'INSERT'  -- Only new version rows
-- ) source
-- ON target.product_id = source.product_id
-- WHEN MATCHED THEN UPDATE SET
--     target.product_name = source.product_name,
--     target.category = source.category,
--     target.unit_price = source.unit_price,
--     target.updated_at = CURRENT_TIMESTAMP()
-- WHEN NOT MATCHED THEN INSERT (...)
--     VALUES (...);

-- -----------------------------------------------------------------------------
-- 4.3 SCD Type 2 Pattern
-- -----------------------------------------------------------------------------
/*
For SCD Type 2, need two operations:
1. Expire current records that changed
2. Insert new versions
*/

-- See stored_procedures.sql for full SCD Type 2 implementation

-- =============================================================================
-- SECTION 5: STREAM STALENESS AND RETENTION
-- =============================================================================
/*
Streams have a "stale" concept:
- Stream offset is stored with table's retention period
- If stream not consumed within retention period, it becomes stale
- Stale streams can't be read (must recreate)

Best practices:
- Ensure stream consumption happens within table retention
- For staging tables with 1-day retention, consume at least daily
- Monitor stream age
*/

-- Check stream metadata
SHOW STREAMS IN SCHEMA STAGING;

-- Get stream details
SELECT
    "name" AS stream_name,
    "table_name" AS source_table,
    "type" AS stream_type,
    "stale" AS is_stale,
    "stale_after" AS stale_after,
    "mode" AS stream_mode
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- =============================================================================
-- SECTION 6: ADVANCED STREAM PATTERNS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 6.1 Multi-Stream Processing (Fan-out)
-- -----------------------------------------------------------------------------
/*
One source table can have multiple streams
- Each stream tracks changes independently
- Useful when multiple downstream processes need same changes
*/

-- Create additional stream for analytics processing
CREATE OR REPLACE STREAM STG_SALES_STREAM_ANALYTICS
    ON TABLE STG_SALES
    APPEND_ONLY = FALSE
    COMMENT = 'Separate stream for analytics layer updates';

-- -----------------------------------------------------------------------------
-- 6.2 Stream on View (Supported with caveats)
-- -----------------------------------------------------------------------------
/*
Streams can be created on views if:
- View is based on a single table
- View doesn't have aggregations, DISTINCT, etc.
- Limited support - prefer streams on tables
*/

-- -----------------------------------------------------------------------------
-- 6.3 Handling Late-Arriving Data
-- -----------------------------------------------------------------------------
/*
Streams naturally handle late data:
- Records appear in stream when INSERT completes
- Order of appearance matches INSERT order
- Use transaction_date from data for proper temporal placement
*/

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: What's the difference between Standard and Append-Only streams?
A1:
    STANDARD STREAM:
    - Tracks INSERT, UPDATE, DELETE
    - Shows UPDATE as DELETE + INSERT pairs
    - Uses more resources
    - Best for: dimension tables, mutable data

    APPEND-ONLY STREAM:
    - Only tracks INSERTs
    - More efficient (less overhead)
    - Best for: fact tables, event logs, immutable data

Q2: How does stream consumption work?
A2:
    - Stream maintains an "offset" (position in change log)
    - When you read stream in DML, offset advances
    - Consumed changes won't appear again
    - This is "exactly-once" semantic
    - If DML fails, offset doesn't advance (retry safe)

Q3: What happens if stream becomes stale?
A3:
    - Stream offset is before table's retention period
    - Stream is unusable (must be recreated)
    - You lose the ability to see those changes
    - Prevention: Consume streams regularly

    Recovery:
    - Recreate stream (captures from current point)
    - May need to do full refresh if changes were missed

Q4: How do you handle UPDATE in streams?
A4:
    Updates appear as two rows:
    1. DELETE with old values (METADATA$ISUPDATE = TRUE)
    2. INSERT with new values (METADATA$ISUPDATE = TRUE)

    To process:
    - Filter: WHERE METADATA$ACTION = 'INSERT'
    - This gives you the new version only
    - For full before/after, join on METADATA$ROW_ID

Q5: Can you have multiple consumers of same changes?
A5:
    Yes! Create multiple streams on same table:
    - Each stream has its own offset
    - Each consumer processes independently
    - Changes are not "shared" between streams

Q6: How do streams relate to Time Travel?
A6:
    - Streams use the same underlying change tracking
    - Stream offset is essentially a timestamp
    - Retention affects both Time Travel and streams
    - Transient tables: Max 1 day stream viability
    - Permanent tables: Up to 90 days (Enterprise)
*/

-- =============================================================================
-- SECTION 7: VERIFY STREAMS
-- =============================================================================

SHOW STREAMS IN SCHEMA STAGING;

-- =============================================================================
-- SECTION 8: GRANT PRIVILEGES
-- =============================================================================

-- Grant stream privileges
GRANT SELECT ON STREAM STG_SALES_STREAM TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON STREAM STG_CUSTOMERS_STREAM TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON STREAM STG_PRODUCTS_STREAM TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON STREAM STG_CLICKSTREAM_STREAM TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON STREAM STG_STORES_STREAM TO ROLE RETAIL_ENGINEER;

-- Future streams
GRANT SELECT ON FUTURE STREAMS IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;
