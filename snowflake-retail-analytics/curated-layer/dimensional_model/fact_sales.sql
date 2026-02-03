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

-- Sample query: Daily sales summary
-- SELECT
--     d.full_date,
--     d.day_name,
--     st.region,
--     COUNT(DISTINCT f.order_id) AS order_count,
--     SUM(f.quantity) AS units_sold,
--     SUM(f.net_amount) AS total_revenue,
--     AVG(f.net_amount) AS avg_order_value
-- FROM FACT_SALES f
-- JOIN DIM_DATE d ON f.date_key = d.date_key
-- JOIN DIM_STORE st ON f.store_key = st.store_key
-- GROUP BY 1, 2, 3
-- ORDER BY 1, 3;
