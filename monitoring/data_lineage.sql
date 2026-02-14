/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DATA LINEAGE
================================================================================
Purpose: Track data flow through the pipeline layers
Concepts: Data lineage, ACCESS_HISTORY, object dependencies, impact analysis

Interview Points:
- Data lineage answers "where did this data come from?"
- Snowflake's ACCESS_HISTORY tracks column-level lineage
- Object dependencies show downstream impact of changes
- Essential for compliance, debugging, and impact analysis
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: OBJECT DEPENDENCY TRACKING
-- =============================================================================

-- Find all objects that depend on a specific table
-- (downstream impact analysis)
SELECT
    REFERENCING_OBJECT_NAME AS dependent_object,
    REFERENCING_OBJECT_TYPE AS object_type,
    REFERENCING_OBJECT_DOMAIN AS domain,
    REFERENCED_OBJECT_NAME AS source_object,
    REFERENCED_OBJECT_TYPE AS source_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCED_DATABASE = 'RETAIL_ANALYTICS_DB'
AND REFERENCED_SCHEMA = 'CURATED'
AND REFERENCED_OBJECT_NAME = 'FACT_SALES'
ORDER BY dependent_object;

-- Find all sources for a specific object
-- (upstream lineage)
SELECT
    REFERENCED_OBJECT_NAME AS source_object,
    REFERENCED_OBJECT_TYPE AS source_type,
    REFERENCED_SCHEMA AS source_schema,
    REFERENCING_OBJECT_NAME AS target_object,
    REFERENCING_OBJECT_TYPE AS target_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCING_DATABASE = 'RETAIL_ANALYTICS_DB'
AND REFERENCING_OBJECT_NAME = 'MV_DAILY_SALES_SUMMARY'
ORDER BY source_object;

-- =============================================================================
-- SECTION 2: FULL PIPELINE LINEAGE MAP
-- =============================================================================

-- Map all object dependencies in the retail analytics database
CREATE OR REPLACE VIEW AUDIT.VW_DATA_LINEAGE_MAP AS
SELECT
    REFERENCED_SCHEMA || '.' || REFERENCED_OBJECT_NAME AS source_object,
    REFERENCED_OBJECT_TYPE AS source_type,
    REFERENCING_SCHEMA || '.' || REFERENCING_OBJECT_NAME AS target_object,
    REFERENCING_OBJECT_TYPE AS target_type,
    REFERENCED_SCHEMA AS source_layer,
    REFERENCING_SCHEMA AS target_layer
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCED_DATABASE = 'RETAIL_ANALYTICS_DB'
AND REFERENCING_DATABASE = 'RETAIL_ANALYTICS_DB'
ORDER BY source_layer, source_object, target_layer, target_object;

-- =============================================================================
-- SECTION 3: COLUMN-LEVEL LINEAGE (ACCESS_HISTORY)
-- =============================================================================

-- Track which columns were read and written
-- (Enterprise Edition feature)
SELECT
    QUERY_ID,
    QUERY_START_TIME,
    USER_NAME,
    DIRECT_OBJECTS_ACCESSED,
    BASE_OBJECTS_ACCESSED,
    OBJECTS_MODIFIED
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE QUERY_START_TIME > DATEADD('day', -1, CURRENT_TIMESTAMP())
AND ARRAY_SIZE(OBJECTS_MODIFIED) > 0  -- Only DML operations
ORDER BY QUERY_START_TIME DESC
LIMIT 50;

-- Detailed column-level read lineage
SELECT
    QUERY_ID,
    USER_NAME,
    obj.value:"objectName"::STRING AS object_accessed,
    obj.value:"objectDomain"::STRING AS object_type,
    col.value:"columnName"::STRING AS column_name
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => BASE_OBJECTS_ACCESSED) obj,
    LATERAL FLATTEN(input => obj.value:"columns") col
WHERE QUERY_START_TIME > DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY QUERY_START_TIME DESC;

-- =============================================================================
-- SECTION 4: PIPELINE LAYER LINEAGE SUMMARY
-- =============================================================================

-- Document the expected data flow
CREATE OR REPLACE VIEW AUDIT.VW_PIPELINE_LINEAGE AS
SELECT * FROM (
    VALUES
    ('S3', 'External Storage', 'LANDING.SALES_STAGE', 'External Stage', 'Snowpipe / COPY INTO'),
    ('S3', 'External Storage', 'LANDING.PRODUCTS_STAGE', 'External Stage', 'COPY INTO'),
    ('S3', 'External Storage', 'LANDING.CUSTOMERS_STAGE', 'External Stage', 'COPY INTO / Snowpipe'),
    ('S3', 'External Storage', 'LANDING.CLICKSTREAM_STAGE', 'External Stage', 'Snowpipe Auto-ingest'),
    ('LANDING.SALES_STAGE', 'External Stage', 'STAGING.STG_SALES', 'Staging Table', 'COPY INTO with validation'),
    ('LANDING.PRODUCTS_STAGE', 'External Stage', 'STAGING.STG_PRODUCTS', 'Staging Table', 'COPY INTO with JSON parsing'),
    ('LANDING.CUSTOMERS_STAGE', 'External Stage', 'STAGING.STG_CUSTOMERS', 'Staging Table', 'COPY INTO with Parquet'),
    ('STAGING.STG_SALES', 'Staging Table', 'CURATED.FACT_SALES', 'Fact Table', 'Stream + Task (SP_LOAD_FACT_SALES)'),
    ('STAGING.STG_CUSTOMERS', 'Staging Table', 'CURATED.DIM_CUSTOMER', 'Dimension', 'Stream + Task (SP_LOAD_DIM_CUSTOMER, SCD2)'),
    ('STAGING.STG_PRODUCTS', 'Staging Table', 'CURATED.DIM_PRODUCT', 'Dimension', 'Stream + Task (SP_LOAD_DIM_PRODUCT, SCD1)'),
    ('STAGING.STG_STORES', 'Staging Table', 'CURATED.DIM_STORE', 'Dimension', 'MERGE (SP_LOAD_DIM_STORE)'),
    ('CURATED.FACT_SALES', 'Fact Table', 'ANALYTICS.MV_DAILY_SALES_SUMMARY', 'Materialized View', 'Auto-refresh'),
    ('CURATED.FACT_SALES', 'Fact Table', 'ANALYTICS.MV_CUSTOMER_360', 'Materialized View', 'Auto-refresh'),
    ('CURATED.FACT_SALES', 'Fact Table', 'ANALYTICS.AGG_DAILY_SALES', 'Aggregate Table', 'Scheduled Task'),
    ('CURATED.FACT_SALES', 'Fact Table', 'ANALYTICS.AGG_MONTHLY_SALES', 'Aggregate Table', 'Scheduled Task')
) AS t(source_object, source_type, target_object, target_type, load_method);

-- =============================================================================
-- SECTION 5: IMPACT ANALYSIS PROCEDURE
-- =============================================================================

-- Procedure to find all downstream objects affected by a table change
CREATE OR REPLACE PROCEDURE SP_IMPACT_ANALYSIS(p_schema VARCHAR, p_object_name VARCHAR)
RETURNS TABLE (level INTEGER, object_path VARCHAR, object_type VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    result := (
        WITH RECURSIVE downstream AS (
            -- Starting point
            SELECT
                1 AS level,
                REFERENCING_SCHEMA || '.' || REFERENCING_OBJECT_NAME AS object_path,
                REFERENCING_OBJECT_TYPE AS object_type,
                REFERENCING_OBJECT_NAME AS object_name,
                REFERENCING_SCHEMA AS object_schema
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE REFERENCED_DATABASE = 'RETAIL_ANALYTICS_DB'
            AND REFERENCED_SCHEMA = p_schema
            AND REFERENCED_OBJECT_NAME = p_object_name

            UNION ALL

            -- Recursive: find objects depending on the dependent objects
            SELECT
                d.level + 1,
                dep.REFERENCING_SCHEMA || '.' || dep.REFERENCING_OBJECT_NAME,
                dep.REFERENCING_OBJECT_TYPE,
                dep.REFERENCING_OBJECT_NAME,
                dep.REFERENCING_SCHEMA
            FROM downstream d
            JOIN SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES dep
                ON dep.REFERENCED_SCHEMA = d.object_schema
                AND dep.REFERENCED_OBJECT_NAME = d.object_name
                AND dep.REFERENCED_DATABASE = 'RETAIL_ANALYTICS_DB'
            WHERE d.level < 5  -- Limit recursion depth
        )
        SELECT DISTINCT level, object_path, object_type
        FROM downstream
        ORDER BY level, object_path
    );
    RETURN TABLE(result);
END;
$$;

-- Usage: CALL SP_IMPACT_ANALYSIS('CURATED', 'FACT_SALES');

-- =============================================================================
-- SECTION 6: GRANT PRIVILEGES
-- =============================================================================

GRANT SELECT ON VIEW AUDIT.VW_DATA_LINEAGE_MAP TO ROLE RETAIL_ADMIN;
GRANT SELECT ON VIEW AUDIT.VW_PIPELINE_LINEAGE TO ROLE RETAIL_ADMIN;
GRANT SELECT ON VIEW AUDIT.VW_PIPELINE_LINEAGE TO ROLE RETAIL_ANALYST;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: What is data lineage and why is it important?
A1: Data lineage tracks data from source to destination:
    - Forward lineage: "What depends on this table?" (impact analysis)
    - Backward lineage: "Where does this data come from?" (root cause)
    - Column-level: Tracks specific column transformations
    Important for: compliance (GDPR), debugging, change management

Q2: How does Snowflake support data lineage?
A2: - OBJECT_DEPENDENCIES: View-level lineage (what references what)
    - ACCESS_HISTORY: Column-level read/write tracking (Enterprise)
    - QUERY_HISTORY: Tracks all queries for audit
    - Tag-based governance: Tag propagation tracks metadata lineage

Q3: How do you perform impact analysis before a schema change?
A3: - Query OBJECT_DEPENDENCIES to find all dependent objects
    - Use recursive CTEs to trace multi-level dependencies
    - Check downstream views, tasks, and procedures
    - Verify materialized views won't break
    - Test changes in a cloned environment first
*/
