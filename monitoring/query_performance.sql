/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - QUERY PERFORMANCE MONITORING
================================================================================
Purpose: Monitor and optimize query performance
Concepts: Query history, query profile, performance tuning

Interview Points:
- Snowflake auto-optimizes, but monitoring helps identify issues
- Query profile shows execution details
- Focus on: spilling, pruning, cache usage
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: SLOW QUERIES (Last 7 Days)
-- =============================================================================

SELECT
    QUERY_ID,
    QUERY_TEXT,
    USER_NAME,
    WAREHOUSE_NAME,
    EXECUTION_TIME / 1000 AS execution_sec,
    BYTES_SCANNED / (1024*1024*1024) AS gb_scanned,
    ROWS_PRODUCED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 2) AS partition_scan_pct,
    BYTES_SPILLED_TO_LOCAL_STORAGE / (1024*1024) AS mb_spilled_local,
    BYTES_SPILLED_TO_REMOTE_STORAGE / (1024*1024) AS mb_spilled_remote,
    QUERY_TYPE,
    START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
AND EXECUTION_TIME > 60000  -- > 1 minute
ORDER BY EXECUTION_TIME DESC
LIMIT 50;

-- =============================================================================
-- SECTION 2: QUERY PATTERNS BY USER
-- =============================================================================

SELECT
    USER_NAME,
    COUNT(*) AS query_count,
    SUM(EXECUTION_TIME) / 1000 / 60 AS total_minutes,
    AVG(EXECUTION_TIME) / 1000 AS avg_seconds,
    MAX(EXECUTION_TIME) / 1000 AS max_seconds,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY USER_NAME
ORDER BY total_minutes DESC;

-- =============================================================================
-- SECTION 3: WAREHOUSE UTILIZATION
-- =============================================================================

SELECT
    WAREHOUSE_NAME,
    DATE_TRUNC('hour', START_TIME) AS hour,
    COUNT(*) AS query_count,
    AVG(EXECUTION_TIME) / 1000 AS avg_execution_sec,
    SUM(CREDITS_USED) AS total_credits,
    AVG(QUEUED_OVERLOAD_TIME) / 1000 AS avg_queue_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -1, CURRENT_TIMESTAMP())
AND WAREHOUSE_NAME IS NOT NULL
GROUP BY 1, 2
ORDER BY 2, 1;

-- =============================================================================
-- SECTION 4: CACHE HIT RATES
-- =============================================================================

SELECT
    DATE_TRUNC('day', START_TIME) AS query_date,
    COUNT(*) AS total_queries,
    SUM(CASE WHEN BYTES_SCANNED = 0 THEN 1 ELSE 0 END) AS result_cache_hits,
    ROUND(SUM(CASE WHEN BYTES_SCANNED = 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS cache_hit_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
AND QUERY_TYPE = 'SELECT'
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- SECTION 5: IDENTIFY QUERIES FOR OPTIMIZATION
-- =============================================================================

-- Queries with poor partition pruning
SELECT
    QUERY_ID,
    QUERY_TEXT,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 2) AS scan_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
AND PARTITIONS_TOTAL > 100
AND PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) > 0.5  -- Scanning >50%
ORDER BY PARTITIONS_SCANNED DESC
LIMIT 20;

-- Queries with spilling (memory issues)
SELECT
    QUERY_ID,
    QUERY_TEXT,
    WAREHOUSE_SIZE,
    BYTES_SPILLED_TO_LOCAL_STORAGE / (1024*1024*1024) AS gb_spilled_local,
    BYTES_SPILLED_TO_REMOTE_STORAGE / (1024*1024*1024) AS gb_spilled_remote
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
ORDER BY BYTES_SPILLED_TO_REMOTE_STORAGE DESC
LIMIT 20;

-- =============================================================================
-- SECTION 6: CREATE MONITORING VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.VW_DAILY_QUERY_METRICS AS
SELECT
    DATE(START_TIME) AS query_date,
    WAREHOUSE_NAME,
    COUNT(*) AS query_count,
    SUM(EXECUTION_TIME) / 1000 / 60 AS total_minutes,
    AVG(EXECUTION_TIME) / 1000 AS avg_seconds,
    SUM(CREDITS_USED) AS total_credits,
    SUM(BYTES_SCANNED) / (1024*1024*1024*1024) AS tb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How do you identify a query that needs optimization?
A1: Look for:
    - High PARTITIONS_SCANNED / PARTITIONS_TOTAL ratio
    - BYTES_SPILLED (needs larger warehouse)
    - Long QUEUED_OVERLOAD_TIME (warehouse undersized)
    - Low cache hit rates

Q2: What causes spilling?
A2: Query needs more memory than warehouse provides.
    Solutions: Larger warehouse, optimize query, filter earlier.

Q3: How does clustering help?
A3: Clustering co-locates related data in micro-partitions.
    Improves partition pruning for filtered queries.
    Most effective for large tables with repeated filter patterns.
*/
