/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DYNAMIC DATA MASKING POLICIES
================================================================================
Purpose: Protect sensitive data with column-level masking
Concepts: Masking policies, conditional masking, PII protection

Interview Points:
- Masking policies apply at query time (dynamic)
- Based on user role, not physical data modification
- Centralized policy management
================================================================================
*/

USE ROLE SECURITYADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;

-- =============================================================================
-- SECTION 1: EMAIL MASKING POLICY
-- =============================================================================

CREATE OR REPLACE MASKING POLICY MASK_EMAIL AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER') THEN val
        WHEN CURRENT_ROLE() = 'RETAIL_ANALYST' THEN
            SUBSTRING(val, 1, 2) || '****@' || SPLIT_PART(val, '@', 2)
        ELSE '****@****.***'
    END;

-- Apply to customer email
ALTER TABLE CURATED.DIM_CUSTOMER MODIFY COLUMN email
    SET MASKING POLICY MASK_EMAIL;

-- =============================================================================
-- SECTION 2: PHONE MASKING POLICY
-- =============================================================================

CREATE OR REPLACE MASKING POLICY MASK_PHONE AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER') THEN val
        ELSE REGEXP_REPLACE(val, '[0-9]', '*', 1, 0)
    END;

ALTER TABLE CURATED.DIM_CUSTOMER MODIFY COLUMN phone
    SET MASKING POLICY MASK_PHONE;

-- =============================================================================
-- SECTION 3: PII MASKING POLICY (Names)
-- =============================================================================

CREATE OR REPLACE MASKING POLICY MASK_NAME AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER') THEN val
        WHEN CURRENT_ROLE() = 'RETAIL_ANALYST' THEN
            SUBSTRING(val, 1, 1) || REPEAT('*', LENGTH(val) - 1)
        ELSE '****'
    END;

-- Apply selectively (not all names need masking)
-- ALTER TABLE CURATED.DIM_CUSTOMER MODIFY COLUMN first_name SET MASKING POLICY MASK_NAME;

-- =============================================================================
-- SECTION 4: CREDIT CARD MASKING (Show last 4)
-- =============================================================================

CREATE OR REPLACE MASKING POLICY MASK_CREDIT_CARD AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN') THEN val
        WHEN LENGTH(val) >= 4 THEN
            REPEAT('*', LENGTH(val) - 4) || RIGHT(val, 4)
        ELSE '****'
    END;

-- =============================================================================
-- SECTION 5: CONDITIONAL MASKING (Based on Data Values)
-- =============================================================================

-- Only mask premium customers' emails for extra protection
CREATE OR REPLACE MASKING POLICY MASK_EMAIL_CONDITIONAL AS (
    email STRING,
    segment STRING
)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() = 'RETAIL_ADMIN' THEN email
        WHEN segment = 'VIP' THEN '****@****.***'
        ELSE SUBSTRING(email, 1, 2) || '****@' || SPLIT_PART(email, '@', 2)
    END;

-- =============================================================================
-- SECTION 6: VERIFY MASKING
-- =============================================================================

-- Show all masking policies
SHOW MASKING POLICIES;

-- Check which columns have masking
SELECT
    policy_name,
    ref_database_name,
    ref_schema_name,
    ref_entity_name,
    ref_column_name
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    POLICY_NAME => 'MASK_EMAIL'
));

-- Test masking (switch roles)
USE ROLE RETAIL_VIEWER;
SELECT customer_id, email, phone FROM CURATED.DIM_CUSTOMER LIMIT 5;

USE ROLE RETAIL_ADMIN;
SELECT customer_id, email, phone FROM CURATED.DIM_CUSTOMER LIMIT 5;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Masking vs Encryption?
A1: - Masking: Display-time obfuscation, data unchanged in storage
    - Encryption: Data encrypted at rest, decrypted for authorized users
    Both can be used together.

Q2: Can users bypass masking?
A2: No. Masking is enforced by Snowflake at query time.
    Even ACCOUNTADMIN sees masked data unless policy allows.

Q3: Performance impact?
A3: Minimal. Masking is applied during query execution,
    not stored or computed in advance.
*/
