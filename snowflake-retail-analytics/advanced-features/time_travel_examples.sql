/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - TIME TRAVEL EXAMPLES
================================================================================
Purpose: Demonstrate Time Travel for historical queries and recovery
Concepts: AT/BEFORE clauses, UNDROP, data recovery

Interview Points:
- Time Travel allows querying historical data states
- Retention: 0-90 days (Enterprise), 0-1 day (Standard)
- Zero additional storage during retention period
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE DEV_WH;

-- =============================================================================
-- SECTION 1: QUERY HISTORICAL DATA
-- =============================================================================

-- Query table as of 1 hour ago
SELECT COUNT(*) AS record_count, MAX(load_timestamp) AS latest_load
FROM FACT_SALES AT(OFFSET => -3600);  -- 3600 seconds = 1 hour

-- Query table as of specific timestamp
SELECT *
FROM DIM_CUSTOMER AT(TIMESTAMP => '2024-01-15 10:00:00'::TIMESTAMP_NTZ)
WHERE customer_id = 'CUST-10001';

-- Query table as of specific statement ID (query_id)
-- SELECT * FROM FACT_SALES AT(STATEMENT => '01a1234b-0001-0000-0000-000000000000');

-- Query table BEFORE a specific statement executed
-- SELECT * FROM FACT_SALES BEFORE(STATEMENT => '01a1234b-0001-0000-0000-000000000000');

-- =============================================================================
-- SECTION 2: COMPARE DATA VERSIONS
-- =============================================================================

-- Find what changed in the last hour
SELECT 'CURRENT' AS version, COUNT(*) AS cnt FROM DIM_CUSTOMER
UNION ALL
SELECT '1_HOUR_AGO', COUNT(*) FROM DIM_CUSTOMER AT(OFFSET => -3600)
UNION ALL
SELECT '1_DAY_AGO', COUNT(*) FROM DIM_CUSTOMER AT(OFFSET => -86400);

-- Find records that were deleted
SELECT curr.customer_id, curr.full_name
FROM DIM_CUSTOMER AT(OFFSET => -3600) AS prev
LEFT JOIN DIM_CUSTOMER AS curr ON prev.customer_key = curr.customer_key
WHERE curr.customer_key IS NULL;

-- Find records that were added
SELECT curr.customer_id, curr.full_name, curr.load_timestamp
FROM DIM_CUSTOMER AS curr
LEFT JOIN DIM_CUSTOMER AT(OFFSET => -3600) AS prev ON curr.customer_key = prev.customer_key
WHERE prev.customer_key IS NULL;

-- =============================================================================
-- SECTION 3: RECOVER DELETED DATA
-- =============================================================================

-- Scenario: Accidentally deleted important records
-- Step 1: Find the records using Time Travel
-- SELECT * FROM DIM_CUSTOMER AT(OFFSET => -3600) WHERE customer_segment = 'VIP';

-- Step 2: Restore the deleted records
-- INSERT INTO DIM_CUSTOMER
-- SELECT * FROM DIM_CUSTOMER AT(OFFSET => -3600)
-- WHERE customer_key NOT IN (SELECT customer_key FROM DIM_CUSTOMER);

-- =============================================================================
-- SECTION 4: UNDROP OBJECTS
-- =============================================================================

-- Accidentally dropped a table? Bring it back!
-- DROP TABLE DIM_CUSTOMER;
-- UNDROP TABLE DIM_CUSTOMER;

-- Accidentally dropped a schema?
-- DROP SCHEMA ANALYTICS;
-- UNDROP SCHEMA ANALYTICS;

-- Accidentally dropped a database?
-- DROP DATABASE RETAIL_ANALYTICS_DB;
-- UNDROP DATABASE RETAIL_ANALYTICS_DB;

-- =============================================================================
-- SECTION 5: CLONE WITH TIME TRAVEL
-- =============================================================================

-- Create a clone of table as it was yesterday
CREATE OR REPLACE TABLE DIM_CUSTOMER_YESTERDAY
    CLONE DIM_CUSTOMER AT(OFFSET => -86400);

-- Clone entire schema from specific time
-- CREATE SCHEMA CURATED_SNAPSHOT CLONE CURATED AT(TIMESTAMP => '2024-01-15 00:00:00');

-- =============================================================================
-- SECTION 6: CHECK DATA RETENTION SETTINGS
-- =============================================================================

-- Show table retention settings
SHOW TABLES LIKE 'DIM%' IN SCHEMA CURATED;

-- Check retention time
SELECT
    TABLE_NAME,
    TABLE_TYPE,
    IS_TRANSIENT,
    RETENTION_TIME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'CURATED';

-- Modify retention (requires proper privileges)
-- ALTER TABLE DIM_CUSTOMER SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: What's the difference between AT and BEFORE?
A1: AT: Returns data at that exact point
    BEFORE: Returns data just before that point (useful for pre-statement state)

Q2: How does Time Travel affect storage costs?
A2: - Data only stored once (micro-partitions are immutable)
    - Changed/deleted data is retained during retention period
    - After retention, data enters Fail-safe (7 days, Snowflake access only)
    - Then data is purged

Q3: Can you extend Time Travel after deletion?
A3: No. Once retention period expires, data moves to Fail-safe.
    After Fail-safe, data is gone forever.
    Plan retention settings before you need them!
*/
