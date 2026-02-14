/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - CLUSTERING KEYS
================================================================================
Purpose: Optimize large table query performance with clustering
Concepts: Clustering keys, micro-partition pruning, reclustering

Interview Points:
- Clustering co-locates related data in micro-partitions
- Improves partition pruning for filter-heavy queries
- Snowflake automatically maintains clustering (background reclustering)
- Most beneficial for tables > 1TB with predictable query patterns
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: CHECK CURRENT CLUSTERING STATUS
-- =============================================================================

-- Check clustering depth and quality for FACT_SALES
SELECT SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES');

-- Check clustering on specific columns
SELECT SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES', '(date_key)');
SELECT SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES', '(date_key, store_key)');
SELECT SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES', '(date_key, customer_key)');

/*
Key metrics from SYSTEM$CLUSTERING_INFORMATION:
- cluster_by_keys: Current clustering key columns
- total_partition_count: Number of micro-partitions
- average_overlaps: Lower is better (0 = perfectly clustered)
- average_depth: Lower is better (1 = perfectly clustered)
- total_constant_partition_count: Partitions with single value (best case)
*/

-- =============================================================================
-- SECTION 2: DEFINE CLUSTERING KEYS FOR FACT TABLE
-- =============================================================================

-- FACT_SALES: Cluster by date (most common filter) and store (second most common)
ALTER TABLE CURATED.FACT_SALES CLUSTER BY (date_key, store_key);

/*
Why (date_key, store_key)?
- date_key: Most queries filter by date range (time-series analysis)
- store_key: Regional analysis is second most common pattern
- Order matters: Most selective filter first
- Limit to 3-4 columns maximum for effectiveness
*/

-- =============================================================================
-- SECTION 3: CLUSTERING FOR DIMENSION TABLES
-- =============================================================================

-- DIM_CUSTOMER: Cluster by is_current and segment (common filter patterns)
ALTER TABLE CURATED.DIM_CUSTOMER CLUSTER BY (is_current, customer_segment);

/*
Why (is_current, customer_segment)?
- is_current: Almost every query filters for current records
- customer_segment: Segment-based analysis is common
- Not clustering by customer_id (high cardinality, not suitable)
*/

-- DIM_PRODUCT: Cluster by category (common GROUP BY/filter column)
ALTER TABLE CURATED.DIM_PRODUCT CLUSTER BY (category, is_active);

-- =============================================================================
-- SECTION 4: VERIFY CLUSTERING IMPROVEMENT
-- =============================================================================

-- Before vs After comparison query
-- Run these queries and check the Query Profile for partition pruning

-- Test 1: Date range filter (should prune most partitions)
SELECT COUNT(*), SUM(net_amount)
FROM CURATED.FACT_SALES
WHERE date_key BETWEEN 20240101 AND 20240131;

-- Test 2: Date + Store filter (should prune even more)
SELECT COUNT(*), SUM(net_amount)
FROM CURATED.FACT_SALES f
JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
WHERE f.date_key BETWEEN 20240101 AND 20240131
AND st.region = 'Northeast';

-- Test 3: Check partition pruning metrics
SELECT
    QUERY_ID,
    QUERY_TEXT,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 2) AS scan_pct
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    RESULT_LIMIT => 10,
    END_TIME_RANGE_START => DATEADD('minute', -5, CURRENT_TIMESTAMP())
))
WHERE QUERY_TEXT LIKE '%FACT_SALES%'
ORDER BY START_TIME DESC;

-- =============================================================================
-- SECTION 5: MONITORING CLUSTERING HEALTH
-- =============================================================================

-- Create a monitoring view for clustering health
CREATE OR REPLACE VIEW AUDIT.VW_CLUSTERING_HEALTH AS
SELECT
    'FACT_SALES' AS table_name,
    PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES')):"average_overlaps"::FLOAT AS avg_overlaps,
    PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES')):"average_depth"::FLOAT AS avg_depth,
    PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES')):"total_partition_count"::INTEGER AS total_partitions,
    CURRENT_TIMESTAMP() AS check_timestamp
UNION ALL
SELECT
    'DIM_CUSTOMER',
    PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('CURATED.DIM_CUSTOMER')):"average_overlaps"::FLOAT,
    PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('CURATED.DIM_CUSTOMER')):"average_depth"::FLOAT,
    PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('CURATED.DIM_CUSTOMER')):"total_partition_count"::INTEGER,
    CURRENT_TIMESTAMP();

-- =============================================================================
-- SECTION 6: RECLUSTERING MANAGEMENT
-- =============================================================================

-- Check automatic reclustering history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
    TABLE_NAME => 'CURATED.FACT_SALES'
))
ORDER BY START_TIME DESC;

-- Suspend automatic clustering (for cost control during development)
-- ALTER TABLE CURATED.FACT_SALES SUSPEND RECLUSTER;

-- Resume automatic clustering
-- ALTER TABLE CURATED.FACT_SALES RESUME RECLUSTER;

-- Remove clustering key (if no longer needed)
-- ALTER TABLE CURATED.FACT_SALES DROP CLUSTERING KEY;

-- =============================================================================
-- SECTION 7: CLUSTERING BEST PRACTICES REFERENCE
-- =============================================================================

/*
WHEN TO USE CLUSTERING:
- Table size > 1 TB (smaller tables don't benefit much)
- Queries consistently filter on specific columns
- High average_depth in CLUSTERING_INFORMATION
- Frequent full table scans visible in query profile

COLUMN SELECTION:
- Choose columns frequently in WHERE clauses
- Low-to-medium cardinality preferred (date, region vs UUID)
- Put most selective column first
- Maximum 3-4 columns recommended

ANTI-PATTERNS:
- Don't cluster small tables (< 100 GB)
- Don't cluster on high-cardinality columns (UUID, email)
- Don't cluster on columns rarely used in filters
- Don't use too many clustering columns (diminishing returns)
*/

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How does clustering differ from traditional indexing?
A1: - No separate index structure (no storage overhead)
    - Works with micro-partition metadata (min/max values)
    - Snowflake automatically maintains clustering
    - No index rebuild or maintenance windows needed
    - Clustering organizes data within micro-partitions

Q2: What is the cost of clustering?
A2: - Automatic reclustering runs in the background
    - Charged as serverless compute credits
    - Credits vary by data volume and change frequency
    - Monitor via AUTOMATIC_CLUSTERING_HISTORY
    - Can suspend/resume to control costs

Q3: How do you choose clustering columns?
A3: - Analyze common query patterns (WHERE, JOIN columns)
    - Use SYSTEM$CLUSTERING_INFORMATION to check current state
    - Start with date/time columns (most queries filter by date)
    - Add 1-2 additional frequently filtered columns
    - Test with QUERY_PROFILE to verify improvement

Q4: Clustering vs Search Optimization vs Materialized Views?
A4: - Clustering: Range queries, time-series, sequential access
    - Search Optimization: Point lookups, equality predicates
    - Materialized Views: Complex aggregations, repeated queries
    These features complement each other and can be used together.
*/
