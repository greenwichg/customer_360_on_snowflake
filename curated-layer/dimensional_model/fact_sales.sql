/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - FACT_SALES
================================================================================
Purpose: Central fact table for sales transactions
Concepts: Fact table design, surrogate key lookups, measures, grain

Interview Points:
- Grain: One row per order line item
- Contains measures (quantity, amounts) and foreign keys to dimensions
- Clustered by date for time-series query performance
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: CREATE FACT TABLE
-- =============================================================================

CREATE OR REPLACE TABLE FACT_SALES (
    -- Surrogate keys (foreign keys to dimensions)
    date_key INTEGER NOT NULL,
    customer_key INTEGER,
    product_key INTEGER,
    store_key INTEGER,

    -- Degenerate dimensions (no separate dim table)
    order_id VARCHAR(50) NOT NULL,
    order_line_id INTEGER NOT NULL,

    -- Measures (facts)
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    discount_amount DECIMAL(12,2),
    gross_amount DECIMAL(12,2),
    net_amount DECIMAL(12,2),

    -- Transaction attributes
    transaction_timestamp TIMESTAMP_NTZ,
    payment_method VARCHAR(50),
    order_status VARCHAR(50),

    -- Audit columns
    source_file VARCHAR(500),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT pk_fact_sales PRIMARY KEY (order_id, order_line_id)
)
CLUSTER BY (date_key, store_key)
COMMENT = 'Fact table for sales transactions - grain: one row per order line';

-- =============================================================================
-- SECTION 2: FACT TABLE LOADING PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_SALES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER := 0;
BEGIN
    IF (SYSTEM$STREAM_HAS_DATA('STAGING.STG_SALES_STREAM') = FALSE) THEN
        RETURN 'No changes to process';
    END IF;

    -- Insert new sales with dimension key lookups
    INSERT INTO FACT_SALES (
        date_key, customer_key, product_key, store_key,
        order_id, order_line_id, quantity, unit_price,
        discount_percent, discount_amount, gross_amount, net_amount,
        transaction_timestamp, payment_method, order_status,
        source_file
    )
    SELECT
        -- Date key lookup
        d.date_key,
        -- Customer key lookup (current version)
        c.customer_key,
        -- Product key lookup
        p.product_key,
        -- Store key lookup
        st.store_key,
        -- Degenerate dimensions
        s.order_id,
        s.order_line_id,
        -- Measures
        s.quantity,
        s.unit_price,
        s.discount_percent,
        ROUND(s.unit_price * s.quantity * (s.discount_percent / 100), 2) AS discount_amount,
        ROUND(s.unit_price * s.quantity, 2) AS gross_amount,
        s.total_amount AS net_amount,
        -- Attributes
        s.transaction_date,
        s.payment_method,
        s.order_status,
        s.source_file
    FROM STAGING.STG_SALES_STREAM s
    -- Dimension lookups
    LEFT JOIN DIM_DATE d ON DATE(s.transaction_date) = d.full_date
    LEFT JOIN DIM_CUSTOMER c ON s.customer_id = c.customer_id AND c.is_current = TRUE
    LEFT JOIN DIM_PRODUCT p ON s.product_id = p.product_id
    LEFT JOIN DIM_STORE st ON s.store_id = st.store_id
    WHERE s.METADATA$ACTION = 'INSERT'
    AND s.dq_is_valid = TRUE;

    GET_DML_NUM_ROWS_AFFECTED(v_rows_inserted);

    RETURN v_rows_inserted || ' rows inserted into FACT_SALES';
END;
$$;

-- =============================================================================
-- SECTION 3: DIM_DATE (Pre-populated Calendar)
-- =============================================================================

CREATE OR REPLACE TABLE DIM_DATE (
    date_key INTEGER NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(10),
    quarter_number INTEGER,
    quarter_name VARCHAR(10),
    year_number INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    PRIMARY KEY (date_key)
)
COMMENT = 'Date dimension - pre-populated calendar';

-- Populate date dimension (5 years)
INSERT INTO DIM_DATE
SELECT
    TO_NUMBER(TO_CHAR(d.date_value, 'YYYYMMDD')) AS date_key,
    d.date_value AS full_date,
    DAYOFWEEK(d.date_value) AS day_of_week,
    DAYNAME(d.date_value) AS day_name,
    DAY(d.date_value) AS day_of_month,
    DAYOFYEAR(d.date_value) AS day_of_year,
    WEEKOFYEAR(d.date_value) AS week_of_year,
    MONTH(d.date_value) AS month_number,
    MONTHNAME(d.date_value) AS month_name,
    QUARTER(d.date_value) AS quarter_number,
    'Q' || QUARTER(d.date_value) AS quarter_name,
    YEAR(d.date_value) AS year_number,
    CASE WHEN DAYOFWEEK(d.date_value) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,  -- Customize with actual holidays
    CASE WHEN MONTH(d.date_value) >= 7 THEN YEAR(d.date_value) + 1 ELSE YEAR(d.date_value) END AS fiscal_year,
    CASE
        WHEN MONTH(d.date_value) IN (7,8,9) THEN 1
        WHEN MONTH(d.date_value) IN (10,11,12) THEN 2
        WHEN MONTH(d.date_value) IN (1,2,3) THEN 3
        ELSE 4
    END AS fiscal_quarter
FROM (
    SELECT DATEADD('day', SEQ4(), '2020-01-01')::DATE AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 2192))  -- ~6 years
) d;

-- =============================================================================
-- SECTION 4: DIM_PRODUCT (SCD Type 1)
-- =============================================================================

CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_key INTEGER AUTOINCREMENT,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(500),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(200),
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    weight_kg DECIMAL(8,3),
    is_active BOOLEAN,
    launch_date DATE,
    attributes VARIANT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (product_key)
)
COMMENT = 'Product dimension with SCD Type 1 (overwrite)';

-- =============================================================================
-- SECTION 5: DIM_STORE
-- =============================================================================

CREATE OR REPLACE TABLE DIM_STORE (
    store_key INTEGER AUTOINCREMENT,
    store_id VARCHAR(50) NOT NULL,
    store_name VARCHAR(200),
    store_type VARCHAR(50),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    manager_name VARCHAR(200),
    open_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (store_key)
)
COMMENT = 'Store/location dimension';

-- Insert from staging
INSERT INTO DIM_STORE (store_id, store_name, store_type, city, state, region, country, is_active, open_date)
SELECT store_id, store_name, store_type, city, state, region, country, is_active, open_date
FROM STAGING.STG_STORES
WHERE dq_is_valid = TRUE;

-- =============================================================================
-- SECTION 6: VERIFY FACT TABLE
-- =============================================================================

-- 6.1: View table structure
DESCRIBE TABLE FACT_SALES;
DESCRIBE TABLE DIM_DATE;
DESCRIBE TABLE DIM_PRODUCT;
DESCRIBE TABLE DIM_STORE;

-- 6.2: Show table metadata
SHOW TABLES LIKE 'FACT_SALES' IN SCHEMA CURATED;
SHOW TABLES LIKE 'DIM_%' IN SCHEMA CURATED;

-- 6.3: Row counts for all tables
SELECT 'FACT_SALES' AS table_name, COUNT(*) AS row_count FROM FACT_SALES
UNION ALL
SELECT 'DIM_DATE', COUNT(*) FROM DIM_DATE
UNION ALL
SELECT 'DIM_PRODUCT', COUNT(*) FROM DIM_PRODUCT
UNION ALL
SELECT 'DIM_STORE', COUNT(*) FROM DIM_STORE
UNION ALL
SELECT 'DIM_CUSTOMER', COUNT(*) FROM DIM_CUSTOMER;

-- 6.4: Check clustering information for fact table
SELECT SYSTEM$CLUSTERING_INFORMATION('CURATED.FACT_SALES', '(date_key, store_key)');

-- 6.5: Verify foreign key relationships (orphan check)
SELECT 'Orphan Customer Keys' AS check_type, COUNT(*) AS orphan_count
FROM FACT_SALES f
WHERE f.customer_key IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM DIM_CUSTOMER c WHERE c.customer_key = f.customer_key)
UNION ALL
SELECT 'Orphan Product Keys', COUNT(*)
FROM FACT_SALES f
WHERE f.product_key IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM DIM_PRODUCT p WHERE p.product_key = f.product_key)
UNION ALL
SELECT 'Orphan Store Keys', COUNT(*)
FROM FACT_SALES f
WHERE f.store_key IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM DIM_STORE s WHERE s.store_key = f.store_key)
UNION ALL
SELECT 'Orphan Date Keys', COUNT(*)
FROM FACT_SALES f
WHERE NOT EXISTS (SELECT 1 FROM DIM_DATE d WHERE d.date_key = f.date_key);

-- 6.6: Data quality summary
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT(DISTINCT product_key) AS unique_products,
    COUNT(DISTINCT store_key) AS unique_stores,
    MIN(transaction_timestamp) AS earliest_transaction,
    MAX(transaction_timestamp) AS latest_transaction,
    SUM(net_amount) AS total_revenue,
    AVG(net_amount) AS avg_order_value
FROM FACT_SALES;

-- 6.7: Sample query - Daily sales summary by region
SELECT
    d.full_date,
    d.day_name,
    st.region,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.quantity) AS units_sold,
    SUM(f.net_amount) AS total_revenue,
    ROUND(AVG(f.net_amount), 2) AS avg_order_value
FROM FACT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
JOIN DIM_STORE st ON f.store_key = st.store_key
GROUP BY 1, 2, 3
ORDER BY 1, 3
LIMIT 20;

-- 6.8: Sample query - Product performance analysis
SELECT
    p.category,
    p.subcategory,
    p.brand,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.quantity) AS units_sold,
    SUM(f.net_amount) AS revenue,
    SUM(f.gross_amount - f.net_amount) AS total_discount_given
FROM FACT_SALES f
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
GROUP BY 1, 2, 3
ORDER BY revenue DESC
LIMIT 20;

-- 6.9: Sample query - Customer purchasing patterns
SELECT
    c.customer_segment,
    c.loyalty_tier,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS total_revenue,
    ROUND(SUM(f.net_amount) / COUNT(DISTINCT c.customer_key), 2) AS revenue_per_customer,
    ROUND(COUNT(DISTINCT f.order_id)::FLOAT / COUNT(DISTINCT c.customer_key), 2) AS orders_per_customer
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY 1, 2
ORDER BY total_revenue DESC;

-- 6.10: Time-series trend analysis
SELECT
    d.year_number,
    d.quarter_name,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS orders,
    SUM(f.net_amount) AS revenue,
    LAG(SUM(f.net_amount)) OVER (ORDER BY d.year_number, d.quarter_number, d.month_number) AS prev_month_revenue,
    ROUND((SUM(f.net_amount) - LAG(SUM(f.net_amount)) OVER (ORDER BY d.year_number, d.quarter_number, d.month_number)) /
          NULLIF(LAG(SUM(f.net_amount)) OVER (ORDER BY d.year_number, d.quarter_number, d.month_number), 0) * 100, 2) AS mom_growth_pct
FROM FACT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.year_number, d.quarter_number, d.quarter_name, d.month_number, d.month_name
ORDER BY d.year_number, d.quarter_number, d.month_number;

-- =============================================================================
-- SECTION 7: GRANT PRIVILEGES
-- =============================================================================

-- Grants for RETAIL_ADMIN (full access)
GRANT ALL PRIVILEGES ON TABLE FACT_SALES TO ROLE RETAIL_ADMIN;
GRANT ALL PRIVILEGES ON TABLE DIM_DATE TO ROLE RETAIL_ADMIN;
GRANT ALL PRIVILEGES ON TABLE DIM_PRODUCT TO ROLE RETAIL_ADMIN;
GRANT ALL PRIVILEGES ON TABLE DIM_STORE TO ROLE RETAIL_ADMIN;

-- Grants for RETAIL_ENGINEER (read/write for ETL)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE FACT_SALES TO ROLE RETAIL_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DIM_DATE TO ROLE RETAIL_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DIM_PRODUCT TO ROLE RETAIL_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DIM_STORE TO ROLE RETAIL_ENGINEER;

-- Grants for RETAIL_ANALYST (read-only)
GRANT SELECT ON TABLE FACT_SALES TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE DIM_DATE TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE DIM_PRODUCT TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE DIM_STORE TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE DIM_CUSTOMER TO ROLE RETAIL_ANALYST;

-- Grants for RETAIL_VIEWER (read-only)
GRANT SELECT ON TABLE FACT_SALES TO ROLE RETAIL_VIEWER;
GRANT SELECT ON TABLE DIM_DATE TO ROLE RETAIL_VIEWER;
GRANT SELECT ON TABLE DIM_PRODUCT TO ROLE RETAIL_VIEWER;
GRANT SELECT ON TABLE DIM_STORE TO ROLE RETAIL_VIEWER;

-- Grant execute on loading procedure
GRANT USAGE ON PROCEDURE SP_LOAD_FACT_SALES() TO ROLE RETAIL_ENGINEER;

-- =============================================================================
-- SECTION 8: INTERVIEW Q&A
-- =============================================================================
/*
Q1: What is the grain of FACT_SALES?
A1: One row per order line item. The grain defines the level of detail stored
    in the fact table. In our case, each row represents a single product
    purchased within an order (identified by order_id + order_line_id).

Q2: Why use surrogate keys (date_key, customer_key) instead of natural keys?
A2: Surrogate keys provide:
    - Integer keys for faster joins (smaller than VARCHAR)
    - Protection against source system key changes
    - Support for SCD Type 2 (multiple versions of same customer)
    - Handling of unknown/missing dimension values (-1 key)

Q3: What are degenerate dimensions?
A3: Degenerate dimensions are dimension attributes stored directly in the fact
    table without a separate dimension table. Examples: order_id, order_line_id,
    invoice_number. They provide context but don't need their own table.

Q4: Why is FACT_SALES clustered by (date_key, store_key)?
A4: Clustering optimizes query performance for common filter patterns:
    - Most queries filter by date range (time-series analysis)
    - Regional/store analysis is the second most common pattern
    - Clustering co-locates related micro-partitions for efficient pruning
    - Use SYSTEM$CLUSTERING_INFORMATION to verify clustering quality

Q5: How do you handle late-arriving facts?
A5: Options include:
    - INSERT with all lookups (may have NULL dimension keys)
    - Use a default/unknown dimension record (key = -1)
    - Queue late facts for reprocessing after dimensions arrive
    - Use MERGE with matching on business keys

Q6: What's the difference between additive, semi-additive, and non-additive measures?
A6: - Additive: Can be summed across all dimensions (quantity, amount)
    - Semi-additive: Can be summed across some dimensions (account balance -
      not additive across time, use point-in-time snapshot)
    - Non-additive: Cannot be summed (ratios, percentages, unit_price)

Q7: Why LEFT JOIN to dimensions instead of INNER JOIN during load?
A7: LEFT JOIN ensures all facts load even if dimension lookup fails:
    - New products not yet in DIM_PRODUCT
    - Unknown/anonymous customers
    - Data quality issues in source
    Resulting NULL keys can trigger alerts for investigation.

Q8: How do you handle fact table updates (order cancellations)?
A8: Options:
    - Type 1: Update the fact record directly (loses history)
    - Type 2: Add a new row with negative amounts (audit trail)
    - Flag column: Mark original as canceled, add reversal row
    - Separate FACT_RETURNS table for better analytics

Q9: What indexing exists on Snowflake fact tables?
A9: Snowflake doesn't use traditional indexes. Instead:
    - Micro-partition metadata (min/max values) enables pruning
    - Clustering keys organize data for better pruning
    - Search Optimization Service for point lookups
    - No need to manage index maintenance

Q10: How do you optimize fact table queries?
A10: Best practices:
     - Filter on clustered columns (date_key) early
     - Use specific column lists (avoid SELECT *)
     - Pre-aggregate in materialized views for common queries
     - Ensure dimension tables have proper keys
     - Monitor with QUERY_PROFILE for bottlenecks
*/
