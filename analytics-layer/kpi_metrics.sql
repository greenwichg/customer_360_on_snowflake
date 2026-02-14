/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - KPI METRICS
================================================================================
Purpose: Business KPI calculations for executive dashboards
Concepts: KPI definitions, metric calculations, dashboard-ready views

Interview Points:
- KPIs should have consistent, documented definitions
- Pre-computed metrics improve dashboard load times
- Views provide real-time KPI calculations
- Tasks refresh KPI snapshots on schedule
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: EXECUTIVE DASHBOARD KPIs (Real-time View)
-- =============================================================================

CREATE OR REPLACE VIEW VW_KPI_EXECUTIVE_SUMMARY AS
WITH current_period AS (
    SELECT
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT f.customer_key) AS active_customers,
        SUM(f.net_amount) AS total_revenue,
        SUM(f.quantity) AS total_units,
        AVG(f.net_amount) AS avg_order_value,
        SUM(f.discount_amount) AS total_discounts,
        COUNT(DISTINCT d.full_date) AS selling_days
    FROM CURATED.FACT_SALES f
    JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
    WHERE d.is_current_month = TRUE
),
previous_period AS (
    SELECT
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT f.customer_key) AS active_customers,
        SUM(f.net_amount) AS total_revenue,
        AVG(f.net_amount) AS avg_order_value
    FROM CURATED.FACT_SALES f
    JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
    WHERE d.month_number = MONTH(DATEADD('month', -1, CURRENT_DATE()))
    AND d.year_number = YEAR(DATEADD('month', -1, CURRENT_DATE()))
)
SELECT
    -- Current period metrics
    c.total_orders AS mtd_orders,
    c.active_customers AS mtd_active_customers,
    c.total_revenue AS mtd_revenue,
    c.total_units AS mtd_units_sold,
    ROUND(c.avg_order_value, 2) AS mtd_avg_order_value,
    ROUND(c.total_discounts / NULLIF(c.total_revenue + c.total_discounts, 0) * 100, 2) AS mtd_discount_rate,
    ROUND(c.total_revenue / NULLIF(c.selling_days, 0), 2) AS avg_daily_revenue,

    -- Month-over-month comparisons
    ROUND((c.total_revenue - p.total_revenue) / NULLIF(p.total_revenue, 0) * 100, 2) AS revenue_mom_change_pct,
    ROUND((c.total_orders - p.total_orders) / NULLIF(p.total_orders, 0) * 100, 2) AS orders_mom_change_pct,
    ROUND((c.active_customers - p.active_customers) / NULLIF(p.active_customers, 0) * 100, 2) AS customers_mom_change_pct
FROM current_period c
CROSS JOIN previous_period p;

-- =============================================================================
-- SECTION 2: CUSTOMER KPIs
-- =============================================================================

CREATE OR REPLACE VIEW VW_KPI_CUSTOMER_METRICS AS
WITH customer_stats AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.customer_segment,
        c.loyalty_tier,
        c.registration_date,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.net_amount) AS lifetime_value,
        MIN(d.full_date) AS first_purchase,
        MAX(d.full_date) AS last_purchase,
        DATEDIFF('day', MAX(d.full_date), CURRENT_DATE()) AS recency_days,
        DATEDIFF('month', c.registration_date, CURRENT_DATE()) AS tenure_months
    FROM CURATED.DIM_CUSTOMER c
    LEFT JOIN CURATED.FACT_SALES f ON c.customer_key = f.customer_key
    LEFT JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
    WHERE c.is_current = TRUE
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    -- Customer base metrics
    COUNT(*) AS total_customers,
    COUNT(CASE WHEN total_orders > 0 THEN 1 END) AS customers_with_purchases,
    ROUND(COUNT(CASE WHEN total_orders > 0 THEN 1 END) / NULLIF(COUNT(*), 0) * 100, 2) AS conversion_rate_pct,

    -- Retention metrics
    COUNT(CASE WHEN recency_days <= 30 THEN 1 END) AS active_last_30d,
    COUNT(CASE WHEN recency_days <= 90 THEN 1 END) AS active_last_90d,
    COUNT(CASE WHEN recency_days > 180 THEN 1 END) AS churned_customers,
    ROUND(COUNT(CASE WHEN recency_days <= 90 THEN 1 END) / NULLIF(COUNT(CASE WHEN total_orders > 0 THEN 1 END), 0) * 100, 2) AS retention_rate_90d_pct,

    -- Value metrics
    ROUND(AVG(lifetime_value), 2) AS avg_customer_ltv,
    ROUND(MEDIAN(lifetime_value), 2) AS median_customer_ltv,
    ROUND(AVG(CASE WHEN total_orders > 0 THEN total_orders END), 2) AS avg_orders_per_customer,

    -- Repeat purchase rate
    ROUND(COUNT(CASE WHEN total_orders > 1 THEN 1 END) / NULLIF(COUNT(CASE WHEN total_orders > 0 THEN 1 END), 0) * 100, 2) AS repeat_purchase_rate_pct
FROM customer_stats;

-- =============================================================================
-- SECTION 3: PRODUCT KPIs
-- =============================================================================

CREATE OR REPLACE VIEW VW_KPI_PRODUCT_METRICS AS
SELECT
    p.category,
    COUNT(DISTINCT p.product_key) AS total_products,
    COUNT(DISTINCT CASE WHEN p.is_active THEN p.product_key END) AS active_products,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.net_amount) AS total_revenue,
    ROUND(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT p.product_key), 0), 2) AS revenue_per_product,
    ROUND(AVG(p.profit_margin), 2) AS avg_profit_margin,

    -- Inventory velocity (orders per product)
    ROUND(COUNT(DISTINCT f.order_id) / NULLIF(COUNT(DISTINCT CASE WHEN p.is_active THEN p.product_key END), 0), 2) AS orders_per_active_product
FROM CURATED.DIM_PRODUCT p
LEFT JOIN CURATED.FACT_SALES f ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- =============================================================================
-- SECTION 4: REGIONAL KPIs
-- =============================================================================

CREATE OR REPLACE VIEW VW_KPI_REGIONAL_PERFORMANCE AS
SELECT
    st.region,
    COUNT(DISTINCT st.store_key) AS store_count,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.net_amount) AS total_revenue,
    ROUND(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT st.store_key), 0), 2) AS revenue_per_store,
    ROUND(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT f.customer_key), 0), 2) AS revenue_per_customer,
    ROUND(AVG(f.net_amount), 2) AS avg_order_value,
    ROUND(AVG(f.discount_percent), 2) AS avg_discount_pct,

    -- Market share
    ROUND(SUM(f.net_amount) / NULLIF((SELECT SUM(net_amount) FROM CURATED.FACT_SALES), 0) * 100, 2) AS revenue_share_pct
FROM CURATED.DIM_STORE st
LEFT JOIN CURATED.FACT_SALES f ON st.store_key = f.store_key
WHERE st.is_active = TRUE
GROUP BY st.region
ORDER BY total_revenue DESC;

-- =============================================================================
-- SECTION 5: KPI SNAPSHOT TABLE (Historical Tracking)
-- =============================================================================

CREATE OR REPLACE TABLE KPI_DAILY_SNAPSHOT (
    snapshot_date DATE NOT NULL,
    kpi_name VARCHAR(100) NOT NULL,
    kpi_category VARCHAR(50),
    kpi_value DECIMAL(15,2),
    kpi_unit VARCHAR(20),
    previous_value DECIMAL(15,2),
    change_pct DECIMAL(8,2),
    PRIMARY KEY (snapshot_date, kpi_name)
)
COMMENT = 'Daily KPI snapshot for trend tracking';

-- =============================================================================
-- SECTION 6: KPI SNAPSHOT PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_CAPTURE_KPI_SNAPSHOT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Revenue KPIs
    INSERT INTO KPI_DAILY_SNAPSHOT (snapshot_date, kpi_name, kpi_category, kpi_value, kpi_unit)
    SELECT
        CURRENT_DATE(),
        'TOTAL_REVENUE_MTD',
        'Revenue',
        SUM(f.net_amount),
        'USD'
    FROM CURATED.FACT_SALES f
    JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
    WHERE d.is_current_month = TRUE;

    -- Customer KPIs
    INSERT INTO KPI_DAILY_SNAPSHOT (snapshot_date, kpi_name, kpi_category, kpi_value, kpi_unit)
    SELECT
        CURRENT_DATE(),
        'ACTIVE_CUSTOMERS_30D',
        'Customer',
        COUNT(DISTINCT customer_key),
        'Count'
    FROM CURATED.FACT_SALES f
    JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
    WHERE d.full_date >= DATEADD('day', -30, CURRENT_DATE());

    -- Order KPIs
    INSERT INTO KPI_DAILY_SNAPSHOT (snapshot_date, kpi_name, kpi_category, kpi_value, kpi_unit)
    SELECT
        CURRENT_DATE(),
        'AVG_ORDER_VALUE_MTD',
        'Sales',
        ROUND(AVG(net_amount), 2),
        'USD'
    FROM CURATED.FACT_SALES f
    JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
    WHERE d.is_current_month = TRUE;

    -- Update change percentages
    UPDATE KPI_DAILY_SNAPSHOT curr
    SET
        previous_value = prev.kpi_value,
        change_pct = ROUND((curr.kpi_value - prev.kpi_value) / NULLIF(prev.kpi_value, 0) * 100, 2)
    FROM KPI_DAILY_SNAPSHOT prev
    WHERE curr.snapshot_date = CURRENT_DATE()
    AND prev.snapshot_date = DATEADD('day', -1, CURRENT_DATE())
    AND curr.kpi_name = prev.kpi_name;

    RETURN 'KPI snapshot captured for ' || CURRENT_DATE();
END;
$$;

-- Schedule daily KPI capture
CREATE OR REPLACE TASK TASK_KPI_SNAPSHOT
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 7 * * * America/New_York'  -- 7 AM ET daily
    COMMENT = 'Daily KPI snapshot capture'
AS
    CALL SP_CAPTURE_KPI_SNAPSHOT();

-- ALTER TASK TASK_KPI_SNAPSHOT RESUME;

-- =============================================================================
-- SECTION 7: GRANT PRIVILEGES
-- =============================================================================

GRANT SELECT ON VIEW VW_KPI_EXECUTIVE_SUMMARY TO ROLE RETAIL_ANALYST;
GRANT SELECT ON VIEW VW_KPI_CUSTOMER_METRICS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON VIEW VW_KPI_PRODUCT_METRICS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON VIEW VW_KPI_REGIONAL_PERFORMANCE TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE KPI_DAILY_SNAPSHOT TO ROLE RETAIL_ANALYST;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How do you define and manage KPIs in a data warehouse?
A1: - Define KPIs with clear business formulas
    - Store definitions in metadata or documentation
    - Compute centrally (not in BI tools) for consistency
    - Track historically via snapshot tables for trend analysis

Q2: Why capture KPI snapshots instead of recalculating?
A2: - Historical values may change due to late-arriving data
    - Snapshots preserve the KPI value as reported at that time
    - Enables "what was reported vs reality" analysis
    - Faster dashboard loads for trend charts
*/
