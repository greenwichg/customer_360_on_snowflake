/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - SEARCH OPTIMIZATION SERVICE
================================================================================
Purpose: Accelerate point lookup and equality search queries
Concepts: Search optimization, access paths, point lookups

Interview Points:
- Search Optimization Service (SOS) is a serverless feature
- Optimizes equality and IN predicates (vs clustering for ranges)
- Maintains a persistent data structure alongside micro-partitions
- Particularly effective for selective queries on large tables
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: ENABLE SEARCH OPTIMIZATION
-- =============================================================================

-- Enable on DIM_CUSTOMER for customer_id lookups
ALTER TABLE CURATED.DIM_CUSTOMER ADD SEARCH OPTIMIZATION
    ON EQUALITY(customer_id),
    ON EQUALITY(email);

-- Enable on FACT_SALES for order_id lookups
ALTER TABLE CURATED.FACT_SALES ADD SEARCH OPTIMIZATION
    ON EQUALITY(order_id);

-- Enable on DIM_PRODUCT for product_id and category lookups
ALTER TABLE CURATED.DIM_PRODUCT ADD SEARCH OPTIMIZATION
    ON EQUALITY(product_id),
    ON EQUALITY(category);

-- =============================================================================
-- SECTION 2: SEARCH OPTIMIZATION FOR VARIANT DATA
-- =============================================================================

-- Enable search optimization on semi-structured data paths
ALTER TABLE CURATED.DIM_PRODUCT ADD SEARCH OPTIMIZATION
    ON EQUALITY(attributes:color::STRING),
    ON EQUALITY(attributes:size::STRING);

-- =============================================================================
-- SECTION 3: SEARCH OPTIMIZATION FOR SUBSTRING/REGEX
-- =============================================================================

-- Enable substring search optimization (LIKE with wildcards)
ALTER TABLE CURATED.DIM_CUSTOMER ADD SEARCH OPTIMIZATION
    ON SUBSTRING(full_name),
    ON SUBSTRING(email);

-- This optimizes queries like:
-- SELECT * FROM DIM_CUSTOMER WHERE full_name LIKE '%Smith%';
-- SELECT * FROM DIM_CUSTOMER WHERE email LIKE '%@gmail.com';

-- =============================================================================
-- SECTION 4: SEARCH OPTIMIZATION FOR GEO
-- =============================================================================

-- Enable geographic search optimization (if applicable)
-- ALTER TABLE CURATED.DIM_STORE ADD SEARCH OPTIMIZATION
--     ON GEO(geo_coordinates);

-- =============================================================================
-- SECTION 5: VERIFY SEARCH OPTIMIZATION STATUS
-- =============================================================================

-- Check search optimization status
DESCRIBE SEARCH OPTIMIZATION ON CURATED.DIM_CUSTOMER;
DESCRIBE SEARCH OPTIMIZATION ON CURATED.FACT_SALES;
DESCRIBE SEARCH OPTIMIZATION ON CURATED.DIM_PRODUCT;

-- Show all tables with search optimization enabled
SELECT
    TABLE_NAME,
    SEARCH_OPTIMIZATION,
    SEARCH_OPTIMIZATION_PROGRESS,
    SEARCH_OPTIMIZATION_BYTES
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'CURATED'
AND SEARCH_OPTIMIZATION = 'ON'
ORDER BY TABLE_NAME;

-- =============================================================================
-- SECTION 6: TEST SEARCH OPTIMIZATION PERFORMANCE
-- =============================================================================

-- Test 1: Point lookup on customer_id (should be very fast)
SELECT * FROM CURATED.DIM_CUSTOMER
WHERE customer_id = 'CUST-10001' AND is_current = TRUE;

-- Test 2: Point lookup on order_id
SELECT * FROM CURATED.FACT_SALES
WHERE order_id = 'ORD-20240115-00001';

-- Test 3: IN predicate query
SELECT * FROM CURATED.DIM_PRODUCT
WHERE product_id IN ('PROD-1001', 'PROD-1002', 'PROD-1003');

-- Test 4: Category equality
SELECT * FROM CURATED.DIM_PRODUCT
WHERE category = 'Electronics';

-- Test 5: Substring search
SELECT * FROM CURATED.DIM_CUSTOMER
WHERE full_name LIKE '%Johnson%' AND is_current = TRUE;

-- Check if search optimization was used (look at Query Profile)
-- The "Pruning" section should show "Search Optimization" was applied

-- =============================================================================
-- SECTION 7: MONITOR SEARCH OPTIMIZATION COSTS
-- =============================================================================

-- Check search optimization maintenance costs
SELECT *
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE())
))
ORDER BY START_TIME DESC;

-- Estimated monthly cost summary
SELECT
    TABLE_NAME,
    SUM(CREDITS_USED) AS total_credits_7d,
    ROUND(SUM(CREDITS_USED) * 4.29, 2) AS estimated_monthly_credits  -- ~4.29 weeks/month
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE())
))
GROUP BY TABLE_NAME
ORDER BY total_credits_7d DESC;

-- =============================================================================
-- SECTION 8: MANAGE SEARCH OPTIMIZATION
-- =============================================================================

-- Remove specific search optimization expressions
-- ALTER TABLE CURATED.DIM_CUSTOMER DROP SEARCH OPTIMIZATION
--     ON EQUALITY(email);

-- Remove all search optimization from a table
-- ALTER TABLE CURATED.DIM_CUSTOMER DROP SEARCH OPTIMIZATION;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: When should you use Search Optimization vs Clustering?
A1: Search Optimization:
    - Point lookups (WHERE id = 'value')
    - Equality and IN predicates
    - Selective queries returning few rows
    - High-cardinality columns (IDs, emails)

    Clustering:
    - Range queries (WHERE date BETWEEN x AND y)
    - Sequential access patterns
    - Low-to-medium cardinality columns
    - Queries scanning significant data portions

    Both can be used together on the same table.

Q2: How does Search Optimization work internally?
A2: - Creates a persistent data structure (search access paths)
    - Maps column values to micro-partitions that contain them
    - Enables precise micro-partition pruning for equality predicates
    - Maintained automatically by Snowflake (serverless)
    - Similar concept to a bloom filter or inverted index

Q3: What is the cost of Search Optimization?
A3: Two cost components:
    - Storage: Additional data structure stored alongside table
    - Compute: Serverless credits for maintaining the structure
    - Costs scale with table size and DML frequency
    - Monitor via SEARCH_OPTIMIZATION_HISTORY function
    - Can be removed if costs outweigh benefits

Q4: Limitations of Search Optimization?
A4: - Enterprise Edition or higher required
    - Additional storage cost for search access paths
    - Maintenance compute costs (serverless)
    - Not effective for range queries (use clustering)
    - Build time required after enabling (not instant)
    - Limited to specific predicate types (equality, IN, LIKE)
*/
