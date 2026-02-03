/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - VIRTUAL WAREHOUSES SETUP
================================================================================
Purpose: Create virtual warehouses for different workloads
Concepts: Warehouse sizing, auto-suspend, auto-resume, scaling policies

Interview Points:
- Warehouses are independent compute clusters
- They can be started/stopped without affecting data
- Costs are per-second billing with 60-second minimum
- Multi-cluster warehouses enable horizontal scaling
================================================================================
*/

-- =============================================================================
-- SECTION 1: DROP EXISTING WAREHOUSES (if recreating)
-- =============================================================================
-- Uncomment these lines if you need to recreate warehouses
-- DROP WAREHOUSE IF EXISTS LOADING_WH;
-- DROP WAREHOUSE IF EXISTS TRANSFORM_WH;
-- DROP WAREHOUSE IF EXISTS ANALYTICS_WH;
-- DROP WAREHOUSE IF EXISTS DEV_WH;

-- =============================================================================
-- SECTION 2: LOADING WAREHOUSE
-- =============================================================================
/*
Purpose: Batch data loading from external sources (S3, files)
Size: X-Small (1 credit/hour)
Rationale:
  - Loading is I/O bound, not compute bound
  - Small warehouse is sufficient for most COPY operations
  - Auto-suspend quickly to save costs during idle periods
*/

CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60                    -- Suspend after 1 minute of inactivity
    AUTO_RESUME = TRUE                   -- Auto-start when query arrives
    INITIALLY_SUSPENDED = TRUE           -- Don't start immediately (save costs)
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1                -- Single cluster for loading
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for batch data loading operations from S3 and external sources';

-- Set resource monitor (will be created in 03_resource_monitors.sql)
-- ALTER WAREHOUSE LOADING_WH SET RESOURCE_MONITOR = DAILY_MONITOR;

-- =============================================================================
-- SECTION 3: TRANSFORMATION WAREHOUSE
-- =============================================================================
/*
Purpose: ETL transformations, stored procedures, tasks
Size: Small (2 credits/hour)
Rationale:
  - Tasks run on schedule and need consistent performance
  - Stored procedures may involve complex joins
  - Slightly larger to handle transformation workloads
*/

CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
WITH
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 120                   -- Suspend after 2 minutes
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2                -- Allow scale-out for heavy ETL
    SCALING_POLICY = 'STANDARD'          -- Scale out immediately when needed
    COMMENT = 'Warehouse for ETL tasks, stored procedures, and transformations';

-- =============================================================================
-- SECTION 4: ANALYTICS WAREHOUSE
-- =============================================================================
/*
Purpose: BI queries, dashboards, ad-hoc analytics
Size: Medium (4 credits/hour)
Rationale:
  - Complex analytical queries need more compute
  - Multiple concurrent users (BI tools, analysts)
  - Multi-cluster for handling concurrent query load
*/

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 300                   -- Suspend after 5 minutes
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4                -- Scale out for concurrent queries
    SCALING_POLICY = 'STANDARD'          -- Immediate scale-out
    COMMENT = 'Warehouse for BI tools, dashboards, and analytical queries';

-- =============================================================================
-- SECTION 5: DEVELOPMENT WAREHOUSE
-- =============================================================================
/*
Purpose: Development, testing, ad-hoc exploration
Size: X-Small (1 credit/hour)
Rationale:
  - Developers don't need large compute for exploration
  - Quick suspend to minimize costs during development
  - Separate from production workloads
*/

CREATE WAREHOUSE IF NOT EXISTS DEV_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60                    -- Quick suspend for dev work
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for development and testing purposes';

-- =============================================================================
-- SECTION 6: VERIFY WAREHOUSE CREATION
-- =============================================================================

-- Show all warehouses with their properties
SHOW WAREHOUSES LIKE '%_WH';

-- Detailed warehouse information
SELECT
    "name" AS warehouse_name,
    "size" AS warehouse_size,
    "min_cluster_count" AS min_clusters,
    "max_cluster_count" AS max_clusters,
    "auto_suspend" AS auto_suspend_seconds,
    "auto_resume" AS auto_resume_enabled,
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- =============================================================================
-- SECTION 7: WAREHOUSE MANAGEMENT EXAMPLES
-- =============================================================================

-- Example: Resize warehouse dynamically (e.g., for heavy load)
-- ALTER WAREHOUSE ANALYTICS_WH SET WAREHOUSE_SIZE = 'LARGE';

-- Example: Resume a suspended warehouse
-- ALTER WAREHOUSE ANALYTICS_WH RESUME;

-- Example: Suspend a running warehouse
-- ALTER WAREHOUSE ANALYTICS_WH SUSPEND;

-- Example: Change auto-suspend timeout
-- ALTER WAREHOUSE DEV_WH SET AUTO_SUSPEND = 120;

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: What factors determine warehouse size selection?
A1: Consider:
    - Query complexity (joins, aggregations)
    - Data volume being processed
    - Concurrency requirements
    - Cost budget
    - SLA requirements for query performance

Q2: When should you use multi-cluster warehouses?
A2: Use multi-cluster when:
    - Multiple concurrent users/queries
    - Query queueing is occurring
    - Need to maintain consistent performance under variable load
    - BI tools with many simultaneous users

Q3: What's the difference between STANDARD and ECONOMY scaling policy?
A3:
    - STANDARD: Scales out immediately when queries queue
    - ECONOMY: Waits 6 minutes before adding clusters (saves costs)
    Choose ECONOMY for cost-sensitive workloads with variable load

Q4: How does auto-suspend affect costs?
A4:
    - Snowflake bills per-second (60-second minimum)
    - Auto-suspend stops billing when warehouse is idle
    - Set shorter timeouts (60s) for dev, longer (300s) for analytics
    - Balance between cost savings and query latency

Q5: Can you resize a warehouse while queries are running?
A5: Yes! Snowflake supports hot resizing:
    - Running queries continue on existing resources
    - New queries use the new size
    - No downtime or interruption
*/

-- =============================================================================
-- SECTION 8: MONITORING WAREHOUSE USAGE (for reference)
-- =============================================================================

-- Query warehouse credit usage (last 7 days)
-- Note: Requires ACCOUNTADMIN or MONITOR privilege
/*
SELECT
    WAREHOUSE_NAME,
    DATE_TRUNC('day', START_TIME) AS usage_date,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_service_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, 2;
*/

-- Check current warehouse state
/*
SELECT
    NAME,
    STATE,
    SIZE,
    RUNNING,
    QUEUED,
    IS_DEFAULT,
    IS_CURRENT
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSES());
*/

COMMENT ON WAREHOUSE LOADING_WH IS 'Batch loading warehouse - XS size, 60s auto-suspend';
COMMENT ON WAREHOUSE TRANSFORM_WH IS 'ETL/Task warehouse - S size, 120s auto-suspend';
COMMENT ON WAREHOUSE ANALYTICS_WH IS 'Analytics/BI warehouse - M size, multi-cluster';
COMMENT ON WAREHOUSE DEV_WH IS 'Development warehouse - XS size, 60s auto-suspend';

-- Final verification
SELECT CURRENT_WAREHOUSE();
