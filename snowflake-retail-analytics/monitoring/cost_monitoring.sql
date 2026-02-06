/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - COST MONITORING
================================================================================
Purpose: Track and optimize Snowflake credit usage and costs
Concepts: Resource monitors, credit tracking, cost allocation, optimization

Interview Points:
- Snowflake costs = Compute + Storage + Data Transfer + Serverless
- Resource monitors prevent runaway costs
- Account usage views provide detailed cost analysis
- Cost optimization requires monitoring and right-sizing
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: WAREHOUSE CREDIT USAGE (Last 30 Days)
-- =============================================================================

-- Daily credit usage by warehouse
SELECT
    WAREHOUSE_NAME,
    DATE(START_TIME) AS usage_date,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_service_credits,
    COUNT(*) AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC;

-- Total credits by warehouse (summary)
SELECT
    WAREHOUSE_NAME,
    ROUND(SUM(CREDITS_USED), 2) AS total_credits_30d,
    ROUND(SUM(CREDITS_USED) * 3, 2) AS estimated_cost_30d,  -- ~$3/credit (adjust per contract)
    ROUND(AVG(CREDITS_USED), 4) AS avg_credits_per_hour,
    MIN(START_TIME) AS first_usage,
    MAX(END_TIME) AS last_usage
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;

-- =============================================================================
-- SECTION 2: STORAGE COSTS
-- =============================================================================

-- Storage usage over time
SELECT
    USAGE_DATE,
    ROUND(STORAGE_BYTES / (1024*1024*1024*1024), 4) AS storage_tb,
    ROUND(STAGE_BYTES / (1024*1024*1024*1024), 4) AS stage_tb,
    ROUND(FAILSAFE_BYTES / (1024*1024*1024*1024), 4) AS failsafe_tb,
    ROUND((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES) / (1024*1024*1024*1024), 4) AS total_tb,
    ROUND((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES) / (1024*1024*1024*1024) * 23, 2) AS estimated_cost  -- ~$23/TB/month (on-demand)
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE USAGE_DATE > DATEADD('day', -30, CURRENT_DATE())
ORDER BY USAGE_DATE DESC;

-- Storage by database
SELECT
    TABLE_CATALOG AS database_name,
    TABLE_SCHEMA AS schema_name,
    COUNT(*) AS table_count,
    ROUND(SUM(BYTES) / (1024*1024*1024), 2) AS active_gb,
    ROUND(SUM(TIME_TRAVEL_BYTES) / (1024*1024*1024), 2) AS time_travel_gb,
    ROUND(SUM(FAILSAFE_BYTES) / (1024*1024*1024), 2) AS failsafe_gb,
    ROUND(SUM(BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES) / (1024*1024*1024), 2) AS total_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE DELETED IS NULL
GROUP BY 1, 2
ORDER BY total_gb DESC;

-- =============================================================================
-- SECTION 3: SERVERLESS COSTS
-- =============================================================================

-- Snowpipe costs
SELECT
    PIPE_NAME,
    DATE(START_TIME) AS usage_date,
    SUM(CREDITS_USED) AS credits_used,
    SUM(FILES_INSERTED) AS files_loaded,
    SUM(BYTES_INSERTED) / (1024*1024*1024) AS gb_loaded
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2 DESC;

-- Automatic clustering costs
SELECT
    TABLE_NAME,
    DATE(START_TIME) AS usage_date,
    SUM(CREDITS_USED) AS credits_used,
    SUM(NUM_BYTES_RECLUSTERED) / (1024*1024*1024) AS gb_reclustered
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2 DESC;

-- Materialized view refresh costs
SELECT
    TABLE_NAME,
    DATE(START_TIME) AS usage_date,
    SUM(CREDITS_USED) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2 DESC;

-- =============================================================================
-- SECTION 4: COST MONITORING VIEWS
-- =============================================================================

-- Comprehensive daily cost view
CREATE OR REPLACE VIEW AUDIT.VW_DAILY_COST_SUMMARY AS
WITH compute_costs AS (
    SELECT
        DATE(START_TIME) AS cost_date,
        'COMPUTE' AS cost_category,
        WAREHOUSE_NAME AS resource_name,
        SUM(CREDITS_USED) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME > DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 3
),
serverless_costs AS (
    SELECT
        DATE(START_TIME) AS cost_date,
        'SNOWPIPE' AS cost_category,
        PIPE_NAME AS resource_name,
        SUM(CREDITS_USED) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
    WHERE START_TIME > DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 3
),
clustering_costs AS (
    SELECT
        DATE(START_TIME) AS cost_date,
        'AUTO_CLUSTERING' AS cost_category,
        TABLE_NAME AS resource_name,
        SUM(CREDITS_USED) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
    WHERE START_TIME > DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 3
)
SELECT * FROM compute_costs
UNION ALL SELECT * FROM serverless_costs
UNION ALL SELECT * FROM clustering_costs
ORDER BY cost_date DESC, credits DESC;

-- =============================================================================
-- SECTION 5: COST OPTIMIZATION RECOMMENDATIONS
-- =============================================================================

-- Find idle warehouses (running but no queries)
SELECT
    WAREHOUSE_NAME,
    COUNT(*) AS hours_active,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CASE WHEN QUERIES_EXECUTED = 0 THEN 1 ELSE 0 END) AS idle_hours,
    ROUND(SUM(CASE WHEN QUERIES_EXECUTED = 0 THEN CREDITS_USED ELSE 0 END), 2) AS wasted_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
HAVING wasted_credits > 0
ORDER BY wasted_credits DESC;

-- Find warehouses that could be downsized
SELECT
    WAREHOUSE_NAME,
    WAREHOUSE_SIZE,
    COUNT(*) AS query_count,
    AVG(EXECUTION_TIME) / 1000 AS avg_execution_sec,
    MAX(EXECUTION_TIME) / 1000 AS max_execution_sec,
    SUM(BYTES_SPILLED_TO_LOCAL_STORAGE) AS bytes_spilled_local,
    SUM(BYTES_SPILLED_TO_REMOTE_STORAGE) AS bytes_spilled_remote
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
AND WAREHOUSE_NAME IS NOT NULL
GROUP BY 1, 2
ORDER BY query_count DESC;

-- =============================================================================
-- SECTION 6: GRANT PRIVILEGES
-- =============================================================================

GRANT SELECT ON VIEW AUDIT.VW_DAILY_COST_SUMMARY TO ROLE RETAIL_ADMIN;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: What are the main cost components in Snowflake?
A1: 1. Compute: Credits per second (warehouse usage)
    2. Storage: $/TB/month (active + time travel + fail-safe)
    3. Data Transfer: $/TB (egress only, cross-region/cloud)
    4. Serverless: Credits for Snowpipe, clustering, MV refresh, etc.
    5. Cloud Services: Free if <10% of compute credits

Q2: How do you reduce compute costs?
A2: - AUTO_SUSPEND warehouses quickly (60 seconds for dev)
    - Right-size warehouses (check for spilling)
    - Use result caching (24-hour, free)
    - Separate workloads (ETL vs analytics warehouses)
    - Optimize queries (reduce scanning with clustering)

Q3: How do you reduce storage costs?
A3: - Use transient tables for staging (no fail-safe)
    - Reduce time travel retention where possible
    - Drop unused clones and temporary data
    - Use external tables for cold data (query S3 directly)
    - Compress data before loading
*/
