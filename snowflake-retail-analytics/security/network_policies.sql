/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - NETWORK POLICIES
================================================================================
Purpose: Restrict Snowflake access to approved IP addresses
Concepts: Network policies, IP whitelisting, account-level and user-level policies

Interview Points:
- Network policies restrict access based on IP address
- Can be applied at account level or user level
- Supports both allow lists and block lists
- Essential for enterprise security compliance
================================================================================
*/

USE ROLE SECURITYADMIN;

-- =============================================================================
-- SECTION 1: ACCOUNT-LEVEL NETWORK POLICY
-- =============================================================================

-- Create a network policy for the entire account
CREATE OR REPLACE NETWORK POLICY RETAIL_ACCOUNT_POLICY
    ALLOWED_IP_LIST = (
        '10.0.0.0/8',           -- Internal corporate network
        '172.16.0.0/12',        -- Internal VPN range
        '192.168.0.0/16',       -- Local development
        '203.0.113.0/24'        -- Office public IP range (example)
    )
    BLOCKED_IP_LIST = (
        '203.0.113.100/32'      -- Specific blocked IP (example)
    )
    COMMENT = 'Account-level network policy for retail analytics';

-- Apply to account (CAUTION: test thoroughly before enabling)
-- ALTER ACCOUNT SET NETWORK_POLICY = RETAIL_ACCOUNT_POLICY;

-- =============================================================================
-- SECTION 2: USER-LEVEL NETWORK POLICIES
-- =============================================================================

-- Service account: Only allow from ETL server IPs
CREATE OR REPLACE NETWORK POLICY ETL_SERVICE_POLICY
    ALLOWED_IP_LIST = (
        '10.0.1.50/32',         -- ETL Server 1
        '10.0.1.51/32',         -- ETL Server 2
        '10.0.1.52/32'          -- ETL Server 3 (failover)
    )
    COMMENT = 'Network policy for ETL service accounts';

-- Apply to ETL service user
-- ALTER USER ETL_SERVICE_USER SET NETWORK_POLICY = ETL_SERVICE_POLICY;

-- BI tool service account: Allow from BI server range
CREATE OR REPLACE NETWORK POLICY BI_SERVICE_POLICY
    ALLOWED_IP_LIST = (
        '10.0.2.0/24'           -- BI server subnet
    )
    COMMENT = 'Network policy for BI tool service accounts';

-- Admin access: Restrict to VPN only
CREATE OR REPLACE NETWORK POLICY ADMIN_POLICY
    ALLOWED_IP_LIST = (
        '172.16.0.0/12'         -- VPN range only
    )
    COMMENT = 'Restrictive policy for admin users (VPN only)';

-- =============================================================================
-- SECTION 3: VERIFY NETWORK POLICIES
-- =============================================================================

-- Show all network policies
SHOW NETWORK POLICIES;

-- Show policy details
DESCRIBE NETWORK POLICY RETAIL_ACCOUNT_POLICY;
DESCRIBE NETWORK POLICY ETL_SERVICE_POLICY;

-- Check which users have network policies
SHOW USERS;
-- Look for NETWORK_POLICY column in output

-- Check account-level policy
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;

-- =============================================================================
-- SECTION 4: TESTING NETWORK POLICIES
-- =============================================================================

/*
IMPORTANT: Test network policies carefully before applying to account!

Steps to test safely:
1. Create the policy
2. Apply to a TEST user first (not account level)
3. Verify the test user can connect from allowed IPs
4. Verify the test user is blocked from disallowed IPs
5. Only then apply to account or production users

Emergency: If you lock yourself out:
- Contact Snowflake Support
- Use ACCOUNTADMIN from an allowed IP
- Have a break-glass procedure documented
*/

-- Test: Apply policy to a test user
-- ALTER USER TEST_USER SET NETWORK_POLICY = RETAIL_ACCOUNT_POLICY;

-- Verify current IP
SELECT CURRENT_IP_ADDRESS();

-- =============================================================================
-- SECTION 5: MODIFY POLICIES
-- =============================================================================

-- Add new IP to allow list
ALTER NETWORK POLICY RETAIL_ACCOUNT_POLICY SET
    ALLOWED_IP_LIST = (
        '10.0.0.0/8',
        '172.16.0.0/12',
        '192.168.0.0/16',
        '203.0.113.0/24',
        '198.51.100.0/24'        -- New office IP range
    );

-- Remove account-level policy (emergency)
-- ALTER ACCOUNT UNSET NETWORK_POLICY;

-- Remove user-level policy
-- ALTER USER ETL_SERVICE_USER UNSET NETWORK_POLICY;

-- Drop a network policy (must be unassigned first)
-- DROP NETWORK POLICY IF EXISTS OLD_POLICY;

-- =============================================================================
-- SECTION 6: AUDIT NETWORK ACCESS
-- =============================================================================

-- Check login history for blocked attempts
SELECT
    USER_NAME,
    CLIENT_IP,
    IS_SUCCESS,
    ERROR_CODE,
    ERROR_MESSAGE,
    EVENT_TIMESTAMP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP > DATEADD('day', -7, CURRENT_TIMESTAMP())
AND IS_SUCCESS = 'NO'
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 50;

-- Successful logins by IP address
SELECT
    CLIENT_IP,
    COUNT(*) AS login_count,
    COUNT(DISTINCT USER_NAME) AS unique_users,
    MIN(EVENT_TIMESTAMP) AS first_login,
    MAX(EVENT_TIMESTAMP) AS last_login
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP > DATEADD('day', -30, CURRENT_TIMESTAMP())
AND IS_SUCCESS = 'YES'
GROUP BY CLIENT_IP
ORDER BY login_count DESC;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Account-level vs user-level network policies?
A1: - Account-level: Applied to all users, baseline security
    - User-level: Overrides account policy for specific users
    - User-level is more restrictive (intersection of both)
    - Best practice: Account-level for broad access, user-level for service accounts

Q2: What happens if I set a wrong policy?
A2: - You could lock out all users (including yourself)
    - Always test on a single user first
    - Keep a break-glass admin with broader access
    - Document the recovery procedure
    - Snowflake Support can help in emergencies

Q3: Can network policies block Snowpipe or data sharing?
A3: - Snowpipe: Uses Snowflake's internal IPs (not affected)
    - Data sharing consumers: Their own network policies apply
    - Third-party tools: Must include their IP ranges
    - Cloud services: Generally use Snowflake-managed IPs
*/
