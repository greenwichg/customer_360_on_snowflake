/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DATABASES AND SCHEMAS SETUP
================================================================================
Purpose: Create database hierarchy with appropriate schema organization
Concepts: Permanent vs Transient databases, schema design, data retention

Interview Points:
- Snowflake has three database types: Permanent, Transient, Temporary
- Transient = No Fail-safe (7 days) = Lower storage costs
- Schema organization follows medallion architecture (Landing → Staging → Curated → Analytics)
================================================================================
*/

-- Use ACCOUNTADMIN for initial setup
USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- SECTION 1: CREATE MAIN ANALYTICS DATABASE (PERMANENT)
-- =============================================================================
/*
Database Type: PERMANENT
- Full Time Travel (up to 90 days on Enterprise)
- Fail-safe protection (7 days)
- Use for production data that needs recovery protection
*/

CREATE DATABASE IF NOT EXISTS RETAIL_ANALYTICS_DB
    DATA_RETENTION_TIME_IN_DAYS = 7      -- Time Travel retention (Standard edition max)
    COMMENT = 'Main database for retail analytics platform - contains all production data';

-- Alternative for Enterprise Edition:
-- CREATE DATABASE RETAIL_ANALYTICS_DB DATA_RETENTION_TIME_IN_DAYS = 90;

-- =============================================================================
-- SECTION 2: CREATE SCHEMAS WITHIN MAIN DATABASE
-- =============================================================================

USE DATABASE RETAIL_ANALYTICS_DB;

-- -----------------------------------------------------------------------------
-- 2.1 LANDING SCHEMA (Transient - Raw Data)
-- -----------------------------------------------------------------------------
/*
Purpose: Raw data landing zone from external sources
Type: TRANSIENT (no fail-safe = lower cost)
Rationale:
  - Raw data can be reloaded from source if needed
  - Transient reduces storage costs by 50%
  - Short Time Travel (1 day) is sufficient
*/

CREATE TRANSIENT SCHEMA IF NOT EXISTS LANDING
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Raw data landing zone - data as-is from source systems';

-- -----------------------------------------------------------------------------
-- 2.2 STAGING SCHEMA (Transient - Cleansed Data)
-- -----------------------------------------------------------------------------
/*
Purpose: Cleansed, validated data (Operational Data Store)
Type: TRANSIENT
Rationale:
  - Can be rebuilt from landing + transformation logic
  - Intermediate layer doesn't need full protection
*/

CREATE TRANSIENT SCHEMA IF NOT EXISTS STAGING
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Cleansed and validated data - operational data store (ODS)';

-- -----------------------------------------------------------------------------
-- 2.3 CURATED SCHEMA (Permanent - Star Schema)
-- -----------------------------------------------------------------------------
/*
Purpose: Dimensional model (facts and dimensions)
Type: PERMANENT (default)
Rationale:
  - Business-critical data with transformations
  - Needs full Time Travel for recovery
  - Needs Fail-safe for compliance
*/

CREATE SCHEMA IF NOT EXISTS CURATED
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Curated dimensional model - star schema with facts and dimensions';

-- -----------------------------------------------------------------------------
-- 2.4 ANALYTICS SCHEMA (Permanent - Aggregations)
-- -----------------------------------------------------------------------------
/*
Purpose: Pre-aggregated data, materialized views, KPIs
Type: PERMANENT
Rationale:
  - Derived data but expensive to rebuild
  - Business-facing layer needs protection
*/

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Analytics layer - aggregations, materialized views, KPIs';

-- -----------------------------------------------------------------------------
-- 2.5 SHARED SCHEMA (Permanent - Data Sharing)
-- -----------------------------------------------------------------------------
/*
Purpose: Data prepared for external sharing
Type: PERMANENT
Rationale:
  - Shared data needs stability and traceability
*/

CREATE SCHEMA IF NOT EXISTS SHARED
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Data prepared for secure sharing with external parties';

-- =============================================================================
-- SECTION 3: CREATE DEVELOPMENT DATABASE (TRANSIENT)
-- =============================================================================
/*
Database Type: TRANSIENT
- Reduced Time Travel (max 1 day)
- No Fail-safe = Lower storage costs
- Perfect for development/testing environments
*/

CREATE TRANSIENT DATABASE IF NOT EXISTS RETAIL_DEV_DB
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Development and testing database - cloned from production';

USE DATABASE RETAIL_DEV_DB;

CREATE TRANSIENT SCHEMA IF NOT EXISTS DEV
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Development schema for testing changes';

CREATE TRANSIENT SCHEMA IF NOT EXISTS QA
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'QA schema for testing before production';

CREATE TRANSIENT SCHEMA IF NOT EXISTS SANDBOX
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Sandbox for ad-hoc exploration';

-- =============================================================================
-- SECTION 4: CREATE UTILITY SCHEMAS
-- =============================================================================

USE DATABASE RETAIL_ANALYTICS_DB;

-- Audit and logging schema
CREATE SCHEMA IF NOT EXISTS AUDIT
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Audit logs, pipeline execution history, data quality results';

-- Metadata schema
CREATE SCHEMA IF NOT EXISTS METADATA
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Pipeline metadata, configurations, and reference data';

-- =============================================================================
-- SECTION 5: VERIFY DATABASE AND SCHEMA CREATION
-- =============================================================================

-- Show all databases
SHOW DATABASES LIKE 'RETAIL%';

-- Show schemas in main database
SHOW SCHEMAS IN DATABASE RETAIL_ANALYTICS_DB;

-- Detailed schema information
SELECT
    CATALOG_NAME AS database_name,
    SCHEMA_NAME,
    IS_TRANSIENT,
    RETENTION_TIME,
    CREATED,
    COMMENT
FROM RETAIL_ANALYTICS_DB.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
ORDER BY CREATED;

-- =============================================================================
-- SECTION 6: SET DEFAULT CONTEXT
-- =============================================================================

-- Set default database and schema for session
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE DEV_WH;

-- Verify context
SELECT
    CURRENT_DATABASE() AS current_db,
    CURRENT_SCHEMA() AS current_schema,
    CURRENT_WAREHOUSE() AS current_wh,
    CURRENT_ROLE() AS current_role;

-- =============================================================================
-- SECTION 7: DATABASE/SCHEMA ORGANIZATION DIAGRAM
-- =============================================================================
/*
RETAIL_ANALYTICS_DB (Permanent)
├── LANDING (Transient)
│   ├── RAW_SALES
│   ├── RAW_CUSTOMERS
│   ├── RAW_PRODUCTS
│   ├── RAW_CLICKSTREAM
│   └── EXTERNAL TABLES → S3
│
├── STAGING (Transient)
│   ├── STG_SALES
│   ├── STG_CUSTOMERS
│   ├── STG_PRODUCTS
│   ├── STG_CLICKSTREAM
│   └── STREAMS (CDC)
│
├── CURATED (Permanent)
│   ├── DIM_CUSTOMER (SCD Type 2)
│   ├── DIM_PRODUCT (SCD Type 1)
│   ├── DIM_DATE
│   ├── DIM_STORE
│   ├── FACT_SALES
│   ├── FACT_CLICKSTREAM
│   └── FACT_INVENTORY
│
├── ANALYTICS (Permanent)
│   ├── MV_DAILY_SALES
│   ├── MV_CUSTOMER_360
│   ├── AGG_MONTHLY_REVENUE
│   ├── VW_SALES_SECURE
│   └── KPI_METRICS
│
├── SHARED (Permanent)
│   └── Partner-facing views
│
├── AUDIT (Permanent)
│   ├── PIPELINE_LOGS
│   ├── DATA_QUALITY_RESULTS
│   └── CHANGE_HISTORY
│
└── METADATA (Permanent)
    ├── CONFIG_PARAMS
    └── REFERENCE_DATA

RETAIL_DEV_DB (Transient)
├── DEV
├── QA
└── SANDBOX
*/

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: What's the difference between Permanent, Transient, and Temporary objects?
A1:
    PERMANENT:
    - Full Time Travel (up to 90 days Enterprise, 1 day Standard)
    - 7-day Fail-safe after Time Travel expires
    - Higher storage cost

    TRANSIENT:
    - Time Travel (max 1 day)
    - NO Fail-safe
    - ~50% lower storage cost
    - Good for intermediate/staging data

    TEMPORARY:
    - Only exists for session duration
    - Automatically dropped when session ends
    - No Time Travel, no Fail-safe
    - Good for session-specific temp tables

Q2: Why use transient schemas for Landing and Staging?
A2:
    - Data can be reloaded from source if lost
    - Intermediate data doesn't need long-term protection
    - Significantly reduces storage costs
    - Raw/staging data often has short retention requirements

Q3: How do you decide on DATA_RETENTION_TIME_IN_DAYS?
A3: Consider:
    - Compliance requirements (some industries need 7+ years)
    - Operational needs (how far back might you need to recover?)
    - Cost trade-off (more retention = more storage)
    - Data criticality (reference data vs transactional)

    Typical values:
    - Landing/Staging: 1 day
    - Curated/Analytics: 7-30 days
    - Audit: 30-90 days

Q4: What happens when Time Travel expires?
A4:
    For Permanent tables:
    - Data moves to Fail-safe (7 days)
    - During Fail-safe, only Snowflake support can recover
    - After Fail-safe, data is purged

    For Transient tables:
    - Data is immediately purged (no Fail-safe)
    - This is why transient is cheaper

Q5: How do you organize schemas in a data lakehouse?
A5: Common patterns:
    - Medallion Architecture: Bronze → Silver → Gold
    - Our approach: Landing → Staging → Curated → Analytics
    - Some add: Raw → Cleansed → Curated → Consumption → Publish

    Key principles:
    - Isolate raw from transformed data
    - Apply progressive quality/business rules
    - Separate internal vs external facing data
*/

-- =============================================================================
-- SECTION 8: CLEANUP COMMANDS (for reference only - do not run)
-- =============================================================================
/*
-- Drop schemas (cascade deletes all objects within)
DROP SCHEMA IF EXISTS RETAIL_ANALYTICS_DB.LANDING CASCADE;
DROP SCHEMA IF EXISTS RETAIL_ANALYTICS_DB.STAGING CASCADE;
DROP SCHEMA IF EXISTS RETAIL_ANALYTICS_DB.CURATED CASCADE;
DROP SCHEMA IF EXISTS RETAIL_ANALYTICS_DB.ANALYTICS CASCADE;

-- Drop databases
DROP DATABASE IF EXISTS RETAIL_ANALYTICS_DB;
DROP DATABASE IF EXISTS RETAIL_DEV_DB;

-- Undrop (if within retention period)
UNDROP DATABASE RETAIL_ANALYTICS_DB;
UNDROP SCHEMA RETAIL_ANALYTICS_DB.CURATED;
*/
