/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DATA SHARING
================================================================================
Purpose: Share data securely with partners and external accounts
Concepts: Secure data sharing, reader accounts, shares, data exchange

Interview Points:
- Data sharing is zero-copy (no data movement or duplication)
- Provider controls what data is shared
- Consumer queries live data (always up-to-date)
- No ETL required on consumer side
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;

-- =============================================================================
-- SECTION 1: CREATE OUTBOUND SHARE
-- =============================================================================

-- Create a share for partner analytics
CREATE OR REPLACE SHARE RETAIL_PARTNER_SHARE
    COMMENT = 'Retail analytics data shared with approved partners';

-- Grant usage on database and schema
GRANT USAGE ON DATABASE RETAIL_ANALYTICS_DB TO SHARE RETAIL_PARTNER_SHARE;
GRANT USAGE ON SCHEMA ANALYTICS TO SHARE RETAIL_PARTNER_SHARE;

-- =============================================================================
-- SECTION 2: SHARE SPECIFIC OBJECTS
-- =============================================================================

-- Share materialized views (pre-aggregated, no raw data)
GRANT SELECT ON VIEW ANALYTICS.MV_DAILY_SALES_SUMMARY TO SHARE RETAIL_PARTNER_SHARE;
GRANT SELECT ON VIEW ANALYTICS.MV_PRODUCT_PERFORMANCE TO SHARE RETAIL_PARTNER_SHARE;

-- Share secure views (row-level security enforced)
GRANT SELECT ON VIEW ANALYTICS.VW_SECURE_PRODUCT_PERFORMANCE TO SHARE RETAIL_PARTNER_SHARE;

-- =============================================================================
-- SECTION 3: CREATE SHARE-SPECIFIC SECURE VIEWS
-- =============================================================================

-- Create a view specifically designed for sharing (no PII)
CREATE OR REPLACE SECURE VIEW ANALYTICS.VW_SHARED_SALES_SUMMARY AS
SELECT
    d.full_date AS sale_date,
    d.month_name,
    d.year_number,
    st.region,
    p.category AS product_category,
    p.brand,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.quantity) AS units_sold,
    SUM(f.net_amount) AS net_revenue,
    ROUND(AVG(f.net_amount), 2) AS avg_order_value
FROM CURATED.FACT_SALES f
JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
JOIN CURATED.DIM_PRODUCT p ON f.product_key = p.product_key
GROUP BY 1, 2, 3, 4, 5, 6;

GRANT SELECT ON VIEW ANALYTICS.VW_SHARED_SALES_SUMMARY TO SHARE RETAIL_PARTNER_SHARE;

-- =============================================================================
-- SECTION 4: ADD CONSUMER ACCOUNTS
-- =============================================================================

-- Add partner account(s) to the share
-- ALTER SHARE RETAIL_PARTNER_SHARE ADD ACCOUNTS = PARTNER_ACCOUNT_LOCATOR;

-- For multiple accounts:
-- ALTER SHARE RETAIL_PARTNER_SHARE ADD ACCOUNTS = ACCT1, ACCT2;

-- =============================================================================
-- SECTION 5: READER ACCOUNT (For Non-Snowflake Partners)
-- =============================================================================

/*
Reader accounts allow non-Snowflake customers to access shared data.
The provider pays for compute costs of reader accounts.

CREATE MANAGED ACCOUNT PARTNER_READER
    ADMIN_NAME = 'partner_admin',
    ADMIN_PASSWORD = 'SecureP@ssw0rd!',
    TYPE = READER,
    COMMENT = 'Reader account for Partner XYZ';

-- Note the account locator from the output, then:
ALTER SHARE RETAIL_PARTNER_SHARE ADD ACCOUNTS = <READER_ACCOUNT_LOCATOR>;
*/

-- =============================================================================
-- SECTION 6: CONSUMER SIDE (Reference)
-- =============================================================================

/*
On the consumer account, create a database from the share:

-- List available shares
SHOW SHARES;

-- Create database from share
CREATE DATABASE PARTNER_RETAIL_DATA FROM SHARE PROVIDER_ACCOUNT.RETAIL_PARTNER_SHARE;

-- Query shared data (read-only)
SELECT * FROM PARTNER_RETAIL_DATA.ANALYTICS.VW_SHARED_SALES_SUMMARY LIMIT 100;
*/

-- =============================================================================
-- SECTION 7: VERIFY SHARE CONFIGURATION
-- =============================================================================

-- Show all shares
SHOW SHARES;

-- Show share details
DESCRIBE SHARE RETAIL_PARTNER_SHARE;

-- Show objects in share
SHOW GRANTS TO SHARE RETAIL_PARTNER_SHARE;

-- =============================================================================
-- SECTION 8: DATA EXCHANGE (Marketplace)
-- =============================================================================
/*
Snowflake Data Marketplace allows publishing data as:
1. Free listings: Publicly available data
2. Paid listings: Commercial data products
3. Personalized listings: Targeted to specific accounts

Steps to publish:
1. Create a share with curated data
2. Go to Data Marketplace in Snowflake UI
3. Create a listing with description, sample queries, and documentation
4. Submit for review
*/

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How does Snowflake data sharing work?
A1: Zero-copy architecture:
    - Provider creates a SHARE object and grants access to tables/views
    - Consumer creates a database FROM SHARE
    - Data is NOT copied - consumer queries provider's storage
    - Always up-to-date (no ETL or sync needed)
    - Provider controls access (can revoke anytime)

Q2: What can you share?
A2: - Tables (permanent, not transient/temporary)
    - Secure views (recommended for controlled access)
    - Secure UDFs
    - Cannot share: stages, pipes, tasks, stored procedures

Q3: What about cross-region/cross-cloud sharing?
A3: - Same region: Instant, no data movement
    - Cross-region/cloud: Uses replication (data is copied)
    - Cross-region incurs data transfer costs
    - Listing replication handles this automatically

Q4: Security considerations?
A4: - Always share secure views (not raw tables)
    - Secure views hide the definition from consumers
    - Use row-level policies to filter data per consumer
    - Consumers cannot see underlying tables or modify data
    - Provider retains full control over shared objects

Q5: Cost implications?
A5: - Provider pays for storage (shared data)
    - Consumer pays for compute (querying shared data)
    - Reader accounts: Provider pays for both storage and compute
    - No data transfer costs within same region
*/
