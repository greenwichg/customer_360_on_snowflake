/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - AGGREGATION TABLES
================================================================================
Purpose: Pre-computed summary/aggregate tables for fast dashboard queries
Concepts: Summary tables, scheduled refresh, aggregation patterns

Interview Points:
- Aggregate tables trade storage for query performance
- Updated on schedule (vs materialized views which auto-refresh)
- Useful when MV limitations prevent desired aggregation
- Support complex window functions not possible in MVs
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: DAILY SALES AGGREGATION
-- =============================================================================

CREATE OR REPLACE TABLE AGG_DAILY_SALES (
    sale_date DATE NOT NULL,
    region VARCHAR(50),
    store_type VARCHAR(50),
    product_category VARCHAR(100),
    customer_segment VARCHAR(50),

    -- Measures
    order_count INTEGER,
    unique_customers INTEGER,
    units_sold INTEGER,
    gross_revenue DECIMAL(15,2),
    total_discounts DECIMAL(15,2),
    net_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(10,2),
    avg_discount_pct DECIMAL(5,2),

    -- Period comparisons (pre-computed)
    prev_day_revenue DECIMAL(15,2),
    dod_revenue_change_pct DECIMAL(8,2),
    same_day_last_week_revenue DECIMAL(15,2),
    wow_revenue_change_pct DECIMAL(8,2),

    -- Audit
    refresh_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (sale_date, region, store_type, product_category, customer_segment)
)
CLUSTER BY (sale_date)
COMMENT = 'Daily sales aggregation with period-over-period metrics';

-- =============================================================================
-- SECTION 2: REFRESH DAILY SALES AGGREGATION
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_REFRESH_AGG_DAILY_SALES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE AGG_DAILY_SALES;

    INSERT INTO AGG_DAILY_SALES (
        sale_date, region, store_type, product_category, customer_segment,
        order_count, unique_customers, units_sold,
        gross_revenue, total_discounts, net_revenue,
        avg_order_value, avg_discount_pct,
        prev_day_revenue, dod_revenue_change_pct,
        same_day_last_week_revenue, wow_revenue_change_pct
    )
    WITH daily_data AS (
        SELECT
            d.full_date AS sale_date,
            st.region,
            st.store_type,
            p.category AS product_category,
            c.customer_segment,
            COUNT(DISTINCT f.order_id) AS order_count,
            COUNT(DISTINCT f.customer_key) AS unique_customers,
            SUM(f.quantity) AS units_sold,
            SUM(f.gross_amount) AS gross_revenue,
            SUM(f.discount_amount) AS total_discounts,
            SUM(f.net_amount) AS net_revenue,
            ROUND(AVG(f.net_amount), 2) AS avg_order_value,
            ROUND(AVG(f.discount_percent), 2) AS avg_discount_pct
        FROM CURATED.FACT_SALES f
        JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
        JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
        JOIN CURATED.DIM_PRODUCT p ON f.product_key = p.product_key
        LEFT JOIN CURATED.DIM_CUSTOMER c ON f.customer_key = c.customer_key AND c.is_current = TRUE
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT
        sale_date, region, store_type, product_category, customer_segment,
        order_count, unique_customers, units_sold,
        gross_revenue, total_discounts, net_revenue,
        avg_order_value, avg_discount_pct,
        LAG(net_revenue) OVER (PARTITION BY region, store_type, product_category, customer_segment ORDER BY sale_date) AS prev_day_revenue,
        ROUND((net_revenue - LAG(net_revenue) OVER (PARTITION BY region, store_type, product_category, customer_segment ORDER BY sale_date))
              / NULLIF(LAG(net_revenue) OVER (PARTITION BY region, store_type, product_category, customer_segment ORDER BY sale_date), 0) * 100, 2) AS dod_revenue_change_pct,
        LAG(net_revenue, 7) OVER (PARTITION BY region, store_type, product_category, customer_segment ORDER BY sale_date) AS same_day_last_week_revenue,
        ROUND((net_revenue - LAG(net_revenue, 7) OVER (PARTITION BY region, store_type, product_category, customer_segment ORDER BY sale_date))
              / NULLIF(LAG(net_revenue, 7) OVER (PARTITION BY region, store_type, product_category, customer_segment ORDER BY sale_date), 0) * 100, 2) AS wow_revenue_change_pct
    FROM daily_data;

    RETURN 'AGG_DAILY_SALES refreshed: ' || (SELECT COUNT(*) FROM AGG_DAILY_SALES) || ' rows';
END;
$$;

-- =============================================================================
-- SECTION 3: MONTHLY SALES AGGREGATION
-- =============================================================================

CREATE OR REPLACE TABLE AGG_MONTHLY_SALES (
    year_number INTEGER,
    month_number INTEGER,
    month_name VARCHAR(10),
    region VARCHAR(50),
    product_category VARCHAR(100),

    -- Measures
    order_count INTEGER,
    unique_customers INTEGER,
    units_sold INTEGER,
    net_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(10,2),

    -- Month-over-month
    prev_month_revenue DECIMAL(15,2),
    mom_change_pct DECIMAL(8,2),

    -- Year-over-year
    same_month_last_year_revenue DECIMAL(15,2),
    yoy_change_pct DECIMAL(8,2),

    -- Cumulative
    ytd_revenue DECIMAL(15,2),

    refresh_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (year_number, month_number, region, product_category)
)
COMMENT = 'Monthly sales aggregation with MoM and YoY comparisons';

-- =============================================================================
-- SECTION 4: REFRESH MONTHLY AGGREGATION
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_REFRESH_AGG_MONTHLY_SALES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE AGG_MONTHLY_SALES;

    INSERT INTO AGG_MONTHLY_SALES (
        year_number, month_number, month_name, region, product_category,
        order_count, unique_customers, units_sold, net_revenue, avg_order_value,
        prev_month_revenue, mom_change_pct,
        same_month_last_year_revenue, yoy_change_pct,
        ytd_revenue
    )
    WITH monthly_data AS (
        SELECT
            d.year_number,
            d.month_number,
            d.month_name,
            st.region,
            p.category AS product_category,
            COUNT(DISTINCT f.order_id) AS order_count,
            COUNT(DISTINCT f.customer_key) AS unique_customers,
            SUM(f.quantity) AS units_sold,
            SUM(f.net_amount) AS net_revenue,
            ROUND(AVG(f.net_amount), 2) AS avg_order_value
        FROM CURATED.FACT_SALES f
        JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
        JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
        JOIN CURATED.DIM_PRODUCT p ON f.product_key = p.product_key
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT
        year_number, month_number, month_name, region, product_category,
        order_count, unique_customers, units_sold, net_revenue, avg_order_value,
        LAG(net_revenue) OVER (PARTITION BY region, product_category ORDER BY year_number, month_number) AS prev_month_revenue,
        ROUND((net_revenue - LAG(net_revenue) OVER (PARTITION BY region, product_category ORDER BY year_number, month_number))
              / NULLIF(LAG(net_revenue) OVER (PARTITION BY region, product_category ORDER BY year_number, month_number), 0) * 100, 2) AS mom_change_pct,
        LAG(net_revenue, 12) OVER (PARTITION BY region, product_category ORDER BY year_number, month_number) AS same_month_last_year_revenue,
        ROUND((net_revenue - LAG(net_revenue, 12) OVER (PARTITION BY region, product_category ORDER BY year_number, month_number))
              / NULLIF(LAG(net_revenue, 12) OVER (PARTITION BY region, product_category ORDER BY year_number, month_number), 0) * 100, 2) AS yoy_change_pct,
        SUM(net_revenue) OVER (PARTITION BY year_number, region, product_category ORDER BY month_number) AS ytd_revenue
    FROM monthly_data;

    RETURN 'AGG_MONTHLY_SALES refreshed: ' || (SELECT COUNT(*) FROM AGG_MONTHLY_SALES) || ' rows';
END;
$$;

-- =============================================================================
-- SECTION 5: CUSTOMER SEGMENT AGGREGATION
-- =============================================================================

CREATE OR REPLACE TABLE AGG_CUSTOMER_SEGMENTS (
    customer_segment VARCHAR(50),
    loyalty_tier VARCHAR(20),
    region VARCHAR(50),

    customer_count INTEGER,
    active_customer_count INTEGER,
    total_orders INTEGER,
    total_revenue DECIMAL(15,2),
    avg_lifetime_value DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    avg_orders_per_customer DECIMAL(8,2),
    avg_days_since_last_purchase DECIMAL(8,1),

    refresh_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_segment, loyalty_tier, region)
)
COMMENT = 'Customer segment aggregation for marketing analysis';

-- =============================================================================
-- SECTION 6: SCHEDULE AGGREGATION REFRESH
-- =============================================================================

-- Daily aggregation refresh task
CREATE OR REPLACE TASK TASK_REFRESH_DAILY_AGG
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 5 * * * America/New_York'  -- 5 AM ET daily
    COMMENT = 'Refresh daily sales aggregation'
AS
    CALL SP_REFRESH_AGG_DAILY_SALES();

-- Monthly aggregation refresh task (runs on 1st of each month)
CREATE OR REPLACE TASK TASK_REFRESH_MONTHLY_AGG
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 6 1 * * America/New_York'  -- 6 AM ET, 1st of month
    COMMENT = 'Refresh monthly sales aggregation'
AS
    CALL SP_REFRESH_AGG_MONTHLY_SALES();

-- Enable tasks
-- ALTER TASK TASK_REFRESH_DAILY_AGG RESUME;
-- ALTER TASK TASK_REFRESH_MONTHLY_AGG RESUME;

-- =============================================================================
-- SECTION 7: GRANT PRIVILEGES
-- =============================================================================

GRANT SELECT ON TABLE AGG_DAILY_SALES TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE AGG_MONTHLY_SALES TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE AGG_CUSTOMER_SEGMENTS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE AGG_DAILY_SALES TO ROLE RETAIL_VIEWER;
GRANT SELECT ON TABLE AGG_MONTHLY_SALES TO ROLE RETAIL_VIEWER;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Aggregate table vs Materialized View?
A1: Aggregate tables:
    - Full control over refresh timing and logic
    - Support complex window functions (LAG, SUM OVER)
    - Can include pre-computed period comparisons
    - Manual refresh required

    Materialized Views:
    - Auto-refresh by Snowflake
    - Limited function support (no window functions)
    - Simpler to maintain
    - Snowflake can auto-route queries to MVs

Q2: Why pre-compute period-over-period metrics?
A2: - Dashboard queries run faster (no runtime computation)
    - Consistent metric definitions across reports
    - Reduces warehouse compute costs for repeated queries
    - Trade-off: Storage increase and refresh maintenance
*/
