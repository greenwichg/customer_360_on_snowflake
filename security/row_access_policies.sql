/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - ROW ACCESS POLICIES
================================================================================
Purpose: Implement row-level security to filter data by user context
Concepts: Row access policies, mapping tables, secure functions

Interview Points:
- Row access policies filter rows at query time
- Users only see rows they're authorized for
- Combines with masking for complete data protection
================================================================================
*/

USE ROLE SECURITYADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;

-- =============================================================================
-- SECTION 1: USER-REGION MAPPING TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS METADATA.USER_REGION_MAPPING (
    user_name VARCHAR(100) PRIMARY KEY,
    region VARCHAR(50),
    access_level VARCHAR(20),  -- 'FULL', 'REGION_ONLY', 'RESTRICTED'
    created_date DATE DEFAULT CURRENT_DATE()
);

-- Insert mappings
INSERT INTO METADATA.USER_REGION_MAPPING VALUES
    ('DATA_ANALYST_01', 'Northeast', 'REGION_ONLY', CURRENT_DATE()),
    ('DATA_ANALYST_02', 'West', 'REGION_ONLY', CURRENT_DATE()),
    ('DATA_ENGINEER_01', NULL, 'FULL', CURRENT_DATE()),
    ('RETAIL_ADMIN_USER', NULL, 'FULL', CURRENT_DATE()),
    ('BI_VIEWER', 'ALL', 'REGION_ONLY', CURRENT_DATE());

-- =============================================================================
-- SECTION 2: ROW ACCESS POLICY - REGION BASED
-- =============================================================================

CREATE OR REPLACE ROW ACCESS POLICY RAP_REGION_ACCESS AS (region_col VARCHAR)
RETURNS BOOLEAN ->
    -- Admin and engineers see all
    CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER', 'ACCOUNTADMIN')
    OR
    -- Check user's region mapping
    EXISTS (
        SELECT 1 FROM METADATA.USER_REGION_MAPPING
        WHERE user_name = CURRENT_USER()
        AND (access_level = 'FULL' OR region = region_col OR region = 'ALL')
    );

-- Apply to store dimension
ALTER TABLE CURATED.DIM_STORE ADD ROW ACCESS POLICY RAP_REGION_ACCESS ON (region);

-- =============================================================================
-- SECTION 3: ROW ACCESS POLICY - CUSTOMER SEGMENT
-- =============================================================================

CREATE TABLE IF NOT EXISTS METADATA.USER_SEGMENT_ACCESS (
    user_name VARCHAR(100),
    segment VARCHAR(50),
    PRIMARY KEY (user_name, segment)
);

INSERT INTO METADATA.USER_SEGMENT_ACCESS VALUES
    ('DATA_ANALYST_01', 'STANDARD', CURRENT_DATE()),
    ('DATA_ANALYST_01', 'PREMIUM', CURRENT_DATE()),
    ('DATA_ANALYST_02', 'VIP', CURRENT_DATE());

CREATE OR REPLACE ROW ACCESS POLICY RAP_SEGMENT_ACCESS AS (segment_col VARCHAR)
RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER')
    OR
    EXISTS (
        SELECT 1 FROM METADATA.USER_SEGMENT_ACCESS
        WHERE user_name = CURRENT_USER()
        AND segment = segment_col
    );

-- =============================================================================
-- SECTION 4: VERIFY ROW ACCESS
-- =============================================================================

-- Show policies
SHOW ROW ACCESS POLICIES;

-- Test as different users
USE ROLE RETAIL_ANALYST;
SELECT DISTINCT region FROM CURATED.DIM_STORE;  -- Should see filtered results

USE ROLE RETAIL_ADMIN;
SELECT DISTINCT region FROM CURATED.DIM_STORE;  -- Should see all regions

-- Check policy references
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    POLICY_NAME => 'RAP_REGION_ACCESS'
));

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Row Access Policy vs Secure View?
A1: - Row Access Policy: Applied to base table, automatic
    - Secure View: Separate object, must query view instead of table
    Row Access is more transparent and harder to bypass.

Q2: Can multiple policies apply to one table?
A2: No. One row access policy per table.
    Use AND/OR logic within the single policy.

Q3: Performance considerations?
A3: - Keep mapping tables small and indexed
    - Use simple boolean logic
    - Test with EXPLAIN to verify filtering
*/
