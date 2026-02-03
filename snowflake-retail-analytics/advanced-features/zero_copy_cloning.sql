/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - ZERO-COPY CLONING
================================================================================
Purpose: Create instant copies of data for dev/test environments
Concepts: Clone tables, schemas, databases; instant, no additional storage

Interview Points:
- Clones are instant (metadata operation only)
- No additional storage until data diverges
- Independent objects after creation
================================================================================
*/

USE ROLE RETAIL_ADMIN;
USE WAREHOUSE DEV_WH;

-- =============================================================================
-- SECTION 1: CLONE TABLE
-- =============================================================================

-- Clone a single table (instant, zero storage initially)
CREATE TABLE RETAIL_DEV_DB.SANDBOX.DIM_CUSTOMER_DEV
    CLONE RETAIL_ANALYTICS_DB.CURATED.DIM_CUSTOMER;

-- Clone with Time Travel (specific point in time)
CREATE TABLE RETAIL_DEV_DB.SANDBOX.DIM_CUSTOMER_YESTERDAY
    CLONE RETAIL_ANALYTICS_DB.CURATED.DIM_CUSTOMER
    AT(OFFSET => -86400);

-- =============================================================================
-- SECTION 2: CLONE SCHEMA (All objects)
-- =============================================================================

-- Clone entire schema for development
CREATE SCHEMA RETAIL_DEV_DB.CURATED_DEV
    CLONE RETAIL_ANALYTICS_DB.CURATED;

-- Clone staging schema for testing
CREATE SCHEMA RETAIL_DEV_DB.STAGING_TEST
    CLONE RETAIL_ANALYTICS_DB.STAGING;

-- =============================================================================
-- SECTION 3: CLONE DATABASE
-- =============================================================================

-- Clone entire database (instant!)
CREATE DATABASE RETAIL_ANALYTICS_QA
    CLONE RETAIL_ANALYTICS_DB;

-- Clone database from specific time (disaster recovery)
-- CREATE DATABASE RETAIL_ANALYTICS_RECOVERY
--     CLONE RETAIL_ANALYTICS_DB AT(TIMESTAMP => '2024-01-15 00:00:00');

-- =============================================================================
-- SECTION 4: DEVELOPMENT WORKFLOW
-- =============================================================================

-- Daily dev environment refresh procedure
CREATE OR REPLACE PROCEDURE SP_REFRESH_DEV_ENVIRONMENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Drop old dev schemas
    DROP SCHEMA IF EXISTS RETAIL_DEV_DB.CURATED_DEV;
    DROP SCHEMA IF EXISTS RETAIL_DEV_DB.STAGING_DEV;

    -- Clone fresh from production
    CREATE SCHEMA RETAIL_DEV_DB.CURATED_DEV
        CLONE RETAIL_ANALYTICS_DB.CURATED;
    CREATE SCHEMA RETAIL_DEV_DB.STAGING_DEV
        CLONE RETAIL_ANALYTICS_DB.STAGING;

    RETURN 'Dev environment refreshed at ' || CURRENT_TIMESTAMP();
END;
$$;

-- =============================================================================
-- SECTION 5: CLONE BEHAVIOR
-- =============================================================================
/*
After cloning:
- Clone is independent (changes don't affect source)
- Storage only used when data diverges
- Clone inherits: data, structure, clustering
- Clone does NOT inherit: privileges (must re-grant)

Example storage:
- Source table: 100 GB
- Clone (instant): 0 GB additional
- After INSERT 10 GB to clone: 10 GB additional
- After DELETE from source: Source reclaims, clone keeps data
*/

-- Check storage for cloned objects
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    BYTES / (1024*1024*1024) AS size_gb,
    ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA LIKE '%_DEV'
ORDER BY BYTES DESC;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How is zero-copy cloning "instant"?
A1: Clone only copies metadata (pointers to micro-partitions).
    Actual data isn't copied. When either object changes,
    only modified micro-partitions create new storage.

Q2: When does clone start using storage?
A2: When data diverges (INSERT, UPDATE, DELETE on either side).
    Copy-on-write: only changed partitions use additional storage.

Q3: Use cases for cloning?
A3: - Dev/test environments (instant prod copy)
    - What-if analysis (clone, test, drop)
    - Backup before risky operations
    - Training environments
    - A/B testing scenarios
*/
