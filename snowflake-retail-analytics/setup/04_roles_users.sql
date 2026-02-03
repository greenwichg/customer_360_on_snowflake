/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - ROLE-BASED ACCESS CONTROL (RBAC) SETUP
================================================================================
Purpose: Implement comprehensive RBAC with role hierarchy
Concepts: Custom roles, privilege inheritance, separation of duties

Interview Points:
- Snowflake uses DAC (Discretionary Access Control) with RBAC
- Roles form a hierarchy; privileges flow upward
- Best practice: Grant privileges to roles, grant roles to users
- System roles: ACCOUNTADMIN > SECURITYADMIN > SYSADMIN > PUBLIC
================================================================================
*/

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- SECTION 1: ROLE HIERARCHY DESIGN
-- =============================================================================
/*
                    ACCOUNTADMIN (system)
                          │
            ┌─────────────┼─────────────┐
            │             │             │
     SECURITYADMIN    SYSADMIN    RETAIL_ADMIN
            │             │             │
            │             │     ┌───────┴───────┐
            │             │     │               │
            │        RETAIL_ENGINEER    RETAIL_ANALYST
            │             │             │
            │             │     ┌───────┴───────┐
            │             │     │               │
            │        RETAIL_DEVELOPER   RETAIL_VIEWER
            │                           │
            └───────────────────────────┘
                                │
                             PUBLIC

Legend:
- RETAIL_ADMIN: Full control of retail databases
- RETAIL_ENGINEER: ETL development, pipeline management
- RETAIL_ANALYST: Read/write analytics, create views
- RETAIL_DEVELOPER: Development/sandbox access
- RETAIL_VIEWER: Read-only access to analytics
*/

-- =============================================================================
-- SECTION 2: CREATE CUSTOM ROLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 RETAIL_ADMIN - Database Administrator
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS RETAIL_ADMIN
    COMMENT = 'Administrator role for retail analytics platform - full database control';

-- Grant to SYSADMIN for hierarchy
GRANT ROLE RETAIL_ADMIN TO ROLE SYSADMIN;

-- -----------------------------------------------------------------------------
-- 2.2 RETAIL_ENGINEER - Data Engineer
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS RETAIL_ENGINEER
    COMMENT = 'Data engineer role - ETL development, pipeline management, all schemas';

-- Grant to RETAIL_ADMIN
GRANT ROLE RETAIL_ENGINEER TO ROLE RETAIL_ADMIN;

-- -----------------------------------------------------------------------------
-- 2.3 RETAIL_ANALYST - Data Analyst
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS RETAIL_ANALYST
    COMMENT = 'Analyst role - read curated/analytics, create views and reports';

-- Grant to RETAIL_ENGINEER (analysts are subset of engineers)
GRANT ROLE RETAIL_ANALYST TO ROLE RETAIL_ENGINEER;

-- -----------------------------------------------------------------------------
-- 2.4 RETAIL_DEVELOPER - Developer
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS RETAIL_DEVELOPER
    COMMENT = 'Developer role - development database access, sandbox environment';

-- Grant to RETAIL_ENGINEER
GRANT ROLE RETAIL_DEVELOPER TO ROLE RETAIL_ENGINEER;

-- -----------------------------------------------------------------------------
-- 2.5 RETAIL_VIEWER - Read-Only
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS RETAIL_VIEWER
    COMMENT = 'Viewer role - read-only access to analytics layer';

-- Grant to RETAIL_ANALYST (viewers are subset of analysts)
GRANT ROLE RETAIL_VIEWER TO ROLE RETAIL_ANALYST;

-- =============================================================================
-- SECTION 3: GRANT WAREHOUSE PRIVILEGES
-- =============================================================================

-- RETAIL_ADMIN: All warehouses with MODIFY
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE LOADING_WH TO ROLE RETAIL_ADMIN;
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE TRANSFORM_WH TO ROLE RETAIL_ADMIN;
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE ANALYTICS_WH TO ROLE RETAIL_ADMIN;
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE DEV_WH TO ROLE RETAIL_ADMIN;

-- RETAIL_ENGINEER: Usage on all, Operate on ETL warehouses
GRANT USAGE ON WAREHOUSE LOADING_WH TO ROLE RETAIL_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE TRANSFORM_WH TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE RETAIL_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE DEV_WH TO ROLE RETAIL_ENGINEER;

-- RETAIL_ANALYST: Usage on analytics warehouse only
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE RETAIL_ANALYST;

-- RETAIL_DEVELOPER: Usage on dev warehouse
GRANT USAGE ON WAREHOUSE DEV_WH TO ROLE RETAIL_DEVELOPER;

-- RETAIL_VIEWER: Usage on analytics warehouse (read queries)
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE RETAIL_VIEWER;

-- =============================================================================
-- SECTION 4: GRANT DATABASE PRIVILEGES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 Main Database (RETAIL_ANALYTICS_DB)
-- -----------------------------------------------------------------------------

-- RETAIL_ADMIN: Full control
GRANT OWNERSHIP ON DATABASE RETAIL_ANALYTICS_DB TO ROLE RETAIL_ADMIN COPY CURRENT GRANTS;
GRANT ALL PRIVILEGES ON DATABASE RETAIL_ANALYTICS_DB TO ROLE RETAIL_ADMIN;

-- RETAIL_ENGINEER: Usage on database
GRANT USAGE ON DATABASE RETAIL_ANALYTICS_DB TO ROLE RETAIL_ENGINEER;

-- RETAIL_ANALYST: Usage on database
GRANT USAGE ON DATABASE RETAIL_ANALYTICS_DB TO ROLE RETAIL_ANALYST;

-- RETAIL_VIEWER: Usage on database
GRANT USAGE ON DATABASE RETAIL_ANALYTICS_DB TO ROLE RETAIL_VIEWER;

-- -----------------------------------------------------------------------------
-- 4.2 Dev Database (RETAIL_DEV_DB)
-- -----------------------------------------------------------------------------

-- RETAIL_ADMIN: Full control
GRANT ALL PRIVILEGES ON DATABASE RETAIL_DEV_DB TO ROLE RETAIL_ADMIN;

-- RETAIL_ENGINEER: Full control on dev
GRANT ALL PRIVILEGES ON DATABASE RETAIL_DEV_DB TO ROLE RETAIL_ENGINEER;

-- RETAIL_DEVELOPER: Usage and create
GRANT USAGE, CREATE SCHEMA ON DATABASE RETAIL_DEV_DB TO ROLE RETAIL_DEVELOPER;

-- =============================================================================
-- SECTION 5: GRANT SCHEMA PRIVILEGES
-- =============================================================================

-- Use main database
USE DATABASE RETAIL_ANALYTICS_DB;

-- -----------------------------------------------------------------------------
-- 5.1 LANDING Schema - Engineers only
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA LANDING TO ROLE RETAIL_ADMIN;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT, CREATE PIPE
    ON SCHEMA LANDING TO ROLE RETAIL_ENGINEER;

-- -----------------------------------------------------------------------------
-- 5.2 STAGING Schema - Engineers only
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA STAGING TO ROLE RETAIL_ADMIN;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STREAM, CREATE TASK, CREATE PROCEDURE
    ON SCHEMA STAGING TO ROLE RETAIL_ENGINEER;

-- -----------------------------------------------------------------------------
-- 5.3 CURATED Schema - Engineers write, Analysts read
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA CURATED TO ROLE RETAIL_ADMIN;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STREAM, CREATE TASK, CREATE PROCEDURE,
      CREATE FUNCTION ON SCHEMA CURATED TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON SCHEMA CURATED TO ROLE RETAIL_ANALYST;

-- -----------------------------------------------------------------------------
-- 5.4 ANALYTICS Schema - Analysts read/write views, Viewers read
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS TO ROLE RETAIL_ADMIN;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE PROCEDURE
    ON SCHEMA ANALYTICS TO ROLE RETAIL_ENGINEER;
GRANT USAGE, CREATE VIEW ON SCHEMA ANALYTICS TO ROLE RETAIL_ANALYST;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE RETAIL_VIEWER;

-- -----------------------------------------------------------------------------
-- 5.5 SHARED Schema - Admin only for sharing
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA SHARED TO ROLE RETAIL_ADMIN;
GRANT USAGE ON SCHEMA SHARED TO ROLE RETAIL_ENGINEER;

-- -----------------------------------------------------------------------------
-- 5.6 AUDIT Schema - Admin and Engineer
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA AUDIT TO ROLE RETAIL_ADMIN;
GRANT USAGE, CREATE TABLE, INSERT ON ALL TABLES IN SCHEMA AUDIT TO ROLE RETAIL_ENGINEER;

-- =============================================================================
-- SECTION 6: GRANT TABLE/VIEW PRIVILEGES (Future Grants)
-- =============================================================================
/*
Future grants automatically apply to objects created in the future.
This is essential for maintaining consistent access as new tables are created.
*/

-- LANDING: Engineer full access
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA LANDING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA LANDING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA LANDING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE FILE FORMATS IN SCHEMA LANDING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE PIPES IN SCHEMA LANDING TO ROLE RETAIL_ENGINEER;

-- STAGING: Engineer full access
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE STREAMS IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TASKS IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN SCHEMA STAGING TO ROLE RETAIL_ENGINEER;

-- CURATED: Engineer full, Analyst read
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA CURATED TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA CURATED TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CURATED TO ROLE RETAIL_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CURATED TO ROLE RETAIL_ANALYST;

-- ANALYTICS: Engineer full, Analyst read/view create, Viewer read
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA ANALYTICS TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE MATERIALIZED VIEWS IN SCHEMA ANALYTICS TO ROLE RETAIL_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYTICS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA ANALYTICS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE RETAIL_VIEWER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYTICS TO ROLE RETAIL_VIEWER;
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA ANALYTICS TO ROLE RETAIL_VIEWER;

-- =============================================================================
-- SECTION 7: CREATE SAMPLE USERS
-- =============================================================================

-- Note: In production, use SSO/SAML integration instead of password auth
-- These are sample users for demonstration

-- Admin user
CREATE USER IF NOT EXISTS RETAIL_ADMIN_USER
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = RETAIL_ADMIN
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    DEFAULT_NAMESPACE = RETAIL_ANALYTICS_DB.CURATED
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Admin user for retail analytics platform';

GRANT ROLE RETAIL_ADMIN TO USER RETAIL_ADMIN_USER;

-- Engineer user
CREATE USER IF NOT EXISTS DATA_ENGINEER_01
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = RETAIL_ENGINEER
    DEFAULT_WAREHOUSE = TRANSFORM_WH
    DEFAULT_NAMESPACE = RETAIL_ANALYTICS_DB.STAGING
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Data engineer user';

GRANT ROLE RETAIL_ENGINEER TO USER DATA_ENGINEER_01;

-- Analyst user
CREATE USER IF NOT EXISTS DATA_ANALYST_01
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = RETAIL_ANALYST
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    DEFAULT_NAMESPACE = RETAIL_ANALYTICS_DB.ANALYTICS
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Data analyst user';

GRANT ROLE RETAIL_ANALYST TO USER DATA_ANALYST_01;

-- Developer user
CREATE USER IF NOT EXISTS DEVELOPER_01
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = RETAIL_DEVELOPER
    DEFAULT_WAREHOUSE = DEV_WH
    DEFAULT_NAMESPACE = RETAIL_DEV_DB.SANDBOX
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Developer user for testing';

GRANT ROLE RETAIL_DEVELOPER TO USER DEVELOPER_01;

-- Viewer user (BI tool service account)
CREATE USER IF NOT EXISTS BI_VIEWER
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = RETAIL_VIEWER
    DEFAULT_WAREHOUSE = ANALYTICS_WH
    DEFAULT_NAMESPACE = RETAIL_ANALYTICS_DB.ANALYTICS
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for BI tools';

GRANT ROLE RETAIL_VIEWER TO USER BI_VIEWER;

-- =============================================================================
-- SECTION 8: VERIFY RBAC SETUP
-- =============================================================================

-- Show all custom roles
SHOW ROLES LIKE 'RETAIL%';

-- Show role hierarchy
SELECT
    "name" AS role_name,
    "owner" AS owned_by,
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Show grants TO a role (what privileges the role has)
SHOW GRANTS TO ROLE RETAIL_ENGINEER;

-- Show grants OF a role (who has this role)
SHOW GRANTS OF ROLE RETAIL_ANALYST;

-- Show all users
SHOW USERS;

-- Test role switching (as current user)
-- USE ROLE RETAIL_ANALYST;
-- SELECT CURRENT_ROLE();

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: Explain Snowflake's role hierarchy and how privileges flow.
A1:
    - Roles form a DAG (Directed Acyclic Graph)
    - When Role A is granted to Role B, B inherits all of A's privileges
    - Users activate one role at a time, but get all inherited privileges
    - System roles hierarchy: ACCOUNTADMIN → SECURITYADMIN → SYSADMIN → PUBLIC
    - Custom roles should be children of SYSADMIN (not ACCOUNTADMIN)

Q2: What's the difference between USAGE and SELECT privileges?
A2:
    - USAGE: Allows using an object (required for databases, schemas, warehouses)
    - SELECT: Allows reading data from tables/views
    - You need both: USAGE on database → USAGE on schema → SELECT on table

Q3: Why use future grants?
A3:
    - Automatically apply privileges to objects created in the future
    - Ensures consistent access control as new tables are added
    - Without future grants, you must manually grant on each new object
    - Best practice: Always set up future grants for production schemas

Q4: How do you implement least privilege access?
A4:
    1. Create specific roles for each job function
    2. Grant minimum necessary privileges to each role
    3. Use role hierarchy to avoid privilege duplication
    4. Separate read vs write roles
    5. Use secure views to limit row/column access
    6. Regular access reviews and cleanup

Q5: What's the difference between GRANT and GRANT OWNERSHIP?
A5:
    - GRANT: Gives specific privileges, original owner retains ownership
    - GRANT OWNERSHIP: Transfers full ownership to new owner
    - COPY CURRENT GRANTS: Preserves existing grants during ownership transfer
    - Only owners can grant privileges on their objects

Q6: How do you handle service account access?
A6:
    - Create specific roles for service accounts
    - Use key-pair authentication instead of passwords
    - Implement network policies to restrict IP access
    - Set up separate warehouses for service accounts
    - Monitor service account activity via QUERY_HISTORY
*/

-- =============================================================================
-- SECTION 9: ROLE MANAGEMENT COMMANDS (Reference)
-- =============================================================================

-- Revoke role from user
-- REVOKE ROLE RETAIL_ANALYST FROM USER DATA_ANALYST_01;

-- Revoke privilege from role
-- REVOKE SELECT ON ALL TABLES IN SCHEMA CURATED FROM ROLE RETAIL_VIEWER;

-- Drop role (must revoke all grants first)
-- DROP ROLE IF EXISTS RETAIL_VIEWER;

-- Disable user
-- ALTER USER DATA_ANALYST_01 SET DISABLED = TRUE;

-- Reset user password
-- ALTER USER DATA_ANALYST_01 SET PASSWORD = 'NewPassword123!' MUST_CHANGE_PASSWORD = TRUE;

-- Check effective privileges (what a role can actually do)
-- This helps troubleshoot access issues
/*
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTEE_NAME = 'RETAIL_ANALYST'
AND DELETED_ON IS NULL;
*/
