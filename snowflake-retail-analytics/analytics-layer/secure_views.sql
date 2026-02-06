/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - SECURE VIEWS
================================================================================
Purpose: Row-level security implementation using secure views
Concepts: Secure views, row-level filtering, role-based data access

Interview Points:
- Secure views hide the view definition from unauthorized users
- Combined with row access policies for defense-in-depth
- View definition is not visible via SHOW VIEWS or GET_DDL
- Prevents data exposure through query optimization tricks
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: REGION-BASED ACCESS MAPPING
-- =============================================================================

-- Table to map users/roles to accessible regions
CREATE OR REPLACE TABLE SECURITY.USER_REGION_ACCESS (
    user_name VARCHAR(200),
    role_name VARCHAR(200),
    region VARCHAR(50),
    granted_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    granted_by VARCHAR(200) DEFAULT CURRENT_USER()
)
COMMENT = 'Maps users and roles to regions they can access';

-- Seed sample access data
INSERT INTO SECURITY.USER_REGION_ACCESS (user_name, role_name, region)
VALUES
    ('REGIONAL_MANAGER_EAST', 'RETAIL_ANALYST', 'Northeast'),
    ('REGIONAL_MANAGER_WEST', 'RETAIL_ANALYST', 'West'),
    ('REGIONAL_MANAGER_SOUTH', 'RETAIL_ANALYST', 'South'),
    ('REGIONAL_MANAGER_MIDWEST', 'RETAIL_ANALYST', 'Midwest'),
    ('VP_SALES', 'RETAIL_ADMIN', 'ALL');

-- =============================================================================
-- SECTION 2: SECURE SALES VIEW (Region-Filtered)
-- =============================================================================

CREATE OR REPLACE SECURE VIEW VW_SECURE_SALES AS
SELECT
    f.order_id,
    f.order_line_id,
    d.full_date AS sale_date,
    d.month_name,
    d.year_number,
    c.full_name AS customer_name,
    c.customer_segment,
    c.loyalty_tier,
    p.product_name,
    p.category,
    p.brand,
    st.store_name,
    st.region,
    st.city,
    st.state,
    f.quantity,
    f.unit_price,
    f.discount_percent,
    f.net_amount,
    f.payment_method
FROM CURATED.FACT_SALES f
JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
JOIN CURATED.DIM_CUSTOMER c ON f.customer_key = c.customer_key AND c.is_current = TRUE
JOIN CURATED.DIM_PRODUCT p ON f.product_key = p.product_key
JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
WHERE
    -- Admin roles see all data
    CURRENT_ROLE() IN ('ACCOUNTADMIN', 'RETAIL_ADMIN')
    OR
    -- Other roles see only their accessible regions
    st.region IN (
        SELECT region FROM SECURITY.USER_REGION_ACCESS
        WHERE (user_name = CURRENT_USER() OR role_name = CURRENT_ROLE())
        AND region != 'ALL'
    )
    OR
    -- Users with 'ALL' access see everything
    EXISTS (
        SELECT 1 FROM SECURITY.USER_REGION_ACCESS
        WHERE (user_name = CURRENT_USER() OR role_name = CURRENT_ROLE())
        AND region = 'ALL'
    );

-- =============================================================================
-- SECTION 3: SECURE CUSTOMER VIEW (PII Protected)
-- =============================================================================

CREATE OR REPLACE SECURE VIEW VW_SECURE_CUSTOMER_360 AS
SELECT
    c.customer_key,
    c.customer_id,
    -- Mask PII based on role
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER') THEN c.full_name
        ELSE SUBSTRING(c.full_name, 1, 1) || '****'
    END AS customer_name,
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER') THEN c.email
        ELSE SUBSTRING(c.email, 1, 2) || '****@' || SPLIT_PART(c.email, '@', 2)
    END AS email,
    -- Non-PII fields visible to all
    c.customer_segment,
    c.loyalty_tier,
    c.city,
    c.state,
    c.region,
    c.registration_date,
    -- Aggregated metrics
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS lifetime_value,
    AVG(f.net_amount) AS avg_order_value,
    MIN(d.full_date) AS first_purchase_date,
    MAX(d.full_date) AS last_purchase_date,
    DATEDIFF('day', MAX(d.full_date), CURRENT_DATE()) AS days_since_last_purchase
FROM CURATED.DIM_CUSTOMER c
LEFT JOIN CURATED.FACT_SALES f ON c.customer_key = f.customer_key
LEFT JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

-- =============================================================================
-- SECTION 4: SECURE PRODUCT PERFORMANCE VIEW
-- =============================================================================

CREATE OR REPLACE SECURE VIEW VW_SECURE_PRODUCT_PERFORMANCE AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.unit_price,
    -- Cost visible only to admin/engineer roles
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER') THEN p.unit_cost
        ELSE NULL
    END AS unit_cost,
    CASE
        WHEN CURRENT_ROLE() IN ('RETAIL_ADMIN', 'RETAIL_ENGINEER') THEN p.profit_margin
        ELSE NULL
    END AS profit_margin,
    COUNT(DISTINCT f.order_id) AS times_ordered,
    SUM(f.quantity) AS units_sold,
    SUM(f.net_amount) AS total_revenue
FROM CURATED.DIM_PRODUCT p
LEFT JOIN CURATED.FACT_SALES f ON p.product_key = f.product_key
WHERE p.is_active = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

-- =============================================================================
-- SECTION 5: GRANT PRIVILEGES
-- =============================================================================

GRANT SELECT ON VIEW VW_SECURE_SALES TO ROLE RETAIL_ANALYST;
GRANT SELECT ON VIEW VW_SECURE_SALES TO ROLE RETAIL_VIEWER;
GRANT SELECT ON VIEW VW_SECURE_CUSTOMER_360 TO ROLE RETAIL_ANALYST;
GRANT SELECT ON VIEW VW_SECURE_CUSTOMER_360 TO ROLE RETAIL_VIEWER;
GRANT SELECT ON VIEW VW_SECURE_PRODUCT_PERFORMANCE TO ROLE RETAIL_ANALYST;
GRANT SELECT ON VIEW VW_SECURE_PRODUCT_PERFORMANCE TO ROLE RETAIL_VIEWER;

-- =============================================================================
-- SECTION 6: VERIFY SECURITY
-- =============================================================================

-- Test as different roles
-- USE ROLE RETAIL_ANALYST;
-- SELECT COUNT(*) FROM VW_SECURE_SALES;  -- Should see only their region
-- SELECT * FROM VW_SECURE_CUSTOMER_360 LIMIT 5;  -- Should see masked PII

-- USE ROLE RETAIL_ADMIN;
-- SELECT COUNT(*) FROM VW_SECURE_SALES;  -- Should see all data
-- SELECT * FROM VW_SECURE_CUSTOMER_360 LIMIT 5;  -- Should see full PII

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: What makes a view "secure"?
A1: A secure view:
    - Hides the view definition from non-owner roles
    - Prevents Snowflake from optimizing away security filters
    - GET_DDL returns NULL for unauthorized users
    - Query plan doesn't reveal underlying logic

Q2: Secure view vs Row Access Policy?
A2: - Secure view: Logic embedded in view definition, users query the view
    - Row Access Policy: Declarative policy attached to table, automatic filtering
    - Best practice: Use both for defense-in-depth
    - Row access policy is more scalable (one policy, many tables)

Q3: Performance impact of secure views?
A3: Minor impact because:
    - Snowflake cannot push predicates through secure views
    - Optimizer has less flexibility
    - Best practice: Pre-filter in the view to minimize data scanned
    - Use materialized views where possible for frequently queried secure views
*/
