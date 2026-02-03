/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - MATERIALIZED VIEWS
================================================================================
Purpose: Pre-computed aggregations for fast analytics queries
Concepts: Materialized views, auto-refresh, query acceleration

Interview Points:
- MVs store pre-computed results (unlike regular views)
- Auto-refresh keeps them in sync with base tables
- Snowflake automatically uses MVs for query optimization
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: DAILY SALES SUMMARY MATERIALIZED VIEW
-- =============================================================================

CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_SALES_SUMMARY
    CLUSTER BY (sales_date)
    COMMENT = 'Pre-aggregated daily sales metrics'
AS
SELECT
    d.full_date AS sales_date,
    d.day_name,
    d.week_of_year,
    d.month_number,
    d.month_name,
    d.year_number,
    d.is_weekend,
    st.region,
    st.store_type,
    p.category AS product_category,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.quantity) AS units_sold,
    SUM(f.gross_amount) AS gross_revenue,
    SUM(f.discount_amount) AS total_discounts,
    SUM(f.net_amount) AS net_revenue,
    AVG(f.net_amount) AS avg_order_value,
    AVG(f.discount_percent) AS avg_discount_pct
FROM CURATED.FACT_SALES f
JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
JOIN CURATED.DIM_PRODUCT p ON f.product_key = p.product_key
GROUP BY 1,2,3,4,5,6,7,8,9,10;

-- =============================================================================
-- SECTION 2: CUSTOMER 360 MATERIALIZED VIEW
-- =============================================================================

CREATE OR REPLACE MATERIALIZED VIEW MV_CUSTOMER_360
    COMMENT = 'Customer 360 view with purchase metrics'
AS
SELECT
    c.customer_key,
    c.customer_id,
    c.full_name,
    c.email,
    c.customer_segment,
    c.loyalty_tier,
    c.city,
    c.state,
    c.region,
    c.registration_date,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items_purchased,
    SUM(f.net_amount) AS lifetime_value,
    AVG(f.net_amount) AS avg_order_value,
    MIN(d.full_date) AS first_purchase_date,
    MAX(d.full_date) AS last_purchase_date,
    DATEDIFF('day', MAX(d.full_date), CURRENT_DATE()) AS days_since_last_purchase,
    DATEDIFF('day', c.registration_date, CURRENT_DATE()) AS customer_tenure_days
FROM CURATED.DIM_CUSTOMER c
LEFT JOIN CURATED.FACT_SALES f ON c.customer_key = f.customer_key
LEFT JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY 1,2,3,4,5,6,7,8,9,10;

-- =============================================================================
-- SECTION 3: PRODUCT PERFORMANCE MATERIALIZED VIEW
-- =============================================================================

CREATE OR REPLACE MATERIALIZED VIEW MV_PRODUCT_PERFORMANCE
    COMMENT = 'Product sales performance metrics'
AS
SELECT
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.unit_price,
    p.unit_cost,
    p.profit_margin,
    COUNT(DISTINCT f.order_id) AS times_ordered,
    COUNT(DISTINCT f.customer_key) AS unique_buyers,
    SUM(f.quantity) AS units_sold,
    SUM(f.net_amount) AS total_revenue,
    SUM(f.quantity * p.unit_cost) AS total_cost,
    SUM(f.net_amount) - SUM(f.quantity * p.unit_cost) AS total_profit,
    AVG(f.quantity) AS avg_quantity_per_order
FROM CURATED.DIM_PRODUCT p
LEFT JOIN CURATED.FACT_SALES f ON p.product_key = f.product_key
GROUP BY 1,2,3,4,5,6,7,8,9;

-- =============================================================================
-- SECTION 4: SECURE VIEWS (Row-Level Security)
-- =============================================================================

-- Secure view that filters by user's region
CREATE OR REPLACE SECURE VIEW VW_SALES_BY_REGION AS
SELECT
    f.*,
    d.full_date,
    st.store_name,
    st.region
FROM CURATED.FACT_SALES f
JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
WHERE CURATED.UDF_CAN_ACCESS_REGION(st.region);

-- =============================================================================
-- SECTION 5: VERIFY AND MONITOR MATERIALIZED VIEWS
-- =============================================================================

-- Show materialized views
SHOW MATERIALIZED VIEWS IN SCHEMA ANALYTICS;

-- Check refresh status
SELECT
    name,
    refresh_state,
    is_secure,
    last_refreshed_on,
    compaction_state
FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEWS())
WHERE table_schema = 'ANALYTICS';

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: MV vs Regular View vs Table?
A1: - Regular View: Just a stored query, no data storage
    - Materialized View: Stores results, auto-refreshes, Snowflake uses for optimization
    - Table: Manual refresh required, full control

Q2: When does MV auto-refresh?
A2: When base tables change and when queried (if stale).
    Snowflake handles refresh automatically.

Q3: MV limitations?
A3: - Can't use CURRENT_DATE, CURRENT_TIME
    - Limited function support
    - Can't use external tables
    - Additional storage cost
*/
