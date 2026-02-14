/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - STORED PROCEDURES
================================================================================
Purpose: Complex business logic encapsulated in reusable procedures
Concepts: SQL procedures, JavaScript procedures, error handling

Interview Points:
- Stored procedures support SQL and JavaScript
- Can include control flow (IF, LOOP, TRY/CATCH)
- Called from tasks, manually, or from applications
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;

-- =============================================================================
-- SECTION 1: REFRESH AGGREGATIONS PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_REFRESH_AGGREGATIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Refresh daily sales aggregation
    CREATE OR REPLACE TABLE ANALYTICS.AGG_DAILY_SALES AS
    SELECT
        d.full_date AS sales_date,
        d.day_name,
        d.month_name,
        d.year_number,
        st.region,
        st.store_name,
        p.category AS product_category,
        COUNT(DISTINCT f.order_id) AS order_count,
        SUM(f.quantity) AS units_sold,
        SUM(f.gross_amount) AS gross_revenue,
        SUM(f.discount_amount) AS total_discounts,
        SUM(f.net_amount) AS net_revenue,
        AVG(f.net_amount) AS avg_order_value
    FROM FACT_SALES f
    JOIN DIM_DATE d ON f.date_key = d.date_key
    JOIN DIM_STORE st ON f.store_key = st.store_key
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key
    GROUP BY 1,2,3,4,5,6,7;

    -- Refresh customer 360 summary
    CREATE OR REPLACE TABLE ANALYTICS.AGG_CUSTOMER_360 AS
    SELECT
        c.customer_key,
        c.customer_id,
        c.full_name,
        c.customer_segment,
        c.loyalty_tier,
        c.city,
        c.state,
        c.region,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.quantity) AS total_items,
        SUM(f.net_amount) AS lifetime_value,
        AVG(f.net_amount) AS avg_order_value,
        MIN(d.full_date) AS first_purchase_date,
        MAX(d.full_date) AS last_purchase_date,
        DATEDIFF('day', MAX(d.full_date), CURRENT_DATE()) AS days_since_last_purchase
    FROM DIM_CUSTOMER c
    LEFT JOIN FACT_SALES f ON c.customer_key = f.customer_key
    LEFT JOIN DIM_DATE d ON f.date_key = d.date_key
    WHERE c.is_current = TRUE
    GROUP BY 1,2,3,4,5,6,7,8;

    RETURN 'Aggregations refreshed successfully';
END;
$$;

-- =============================================================================
-- SECTION 2: DATA QUALITY CHECKS PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_DATA_QUALITY_CHECKS()
RETURNS TABLE (check_name VARCHAR, status VARCHAR, error_count INTEGER, details VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    -- Create temp table for results
    CREATE OR REPLACE TEMPORARY TABLE TEMP_DQ_RESULTS (
        check_name VARCHAR,
        status VARCHAR,
        error_count INTEGER,
        details VARCHAR,
        check_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

    -- Check 1: Orphan fact records (no matching dimension)
    INSERT INTO TEMP_DQ_RESULTS (check_name, status, error_count, details)
    SELECT
        'Orphan Customer Keys',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        'Fact records without matching customer dimension'
    FROM FACT_SALES f
    WHERE f.customer_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM DIM_CUSTOMER c WHERE c.customer_key = f.customer_key);

    -- Check 2: Negative amounts
    INSERT INTO TEMP_DQ_RESULTS (check_name, status, error_count, details)
    SELECT
        'Negative Net Amounts',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        'Sales with negative net_amount'
    FROM FACT_SALES
    WHERE net_amount < 0;

    -- Check 3: Future transaction dates
    INSERT INTO TEMP_DQ_RESULTS (check_name, status, error_count, details)
    SELECT
        'Future Transaction Dates',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        'Transactions dated in the future'
    FROM FACT_SALES f
    JOIN DIM_DATE d ON f.date_key = d.date_key
    WHERE d.full_date > CURRENT_DATE();

    -- Check 4: Duplicate orders
    INSERT INTO TEMP_DQ_RESULTS (check_name, status, error_count, details)
    SELECT
        'Duplicate Order Lines',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*),
        'Duplicate order_id + order_line_id combinations'
    FROM (
        SELECT order_id, order_line_id, COUNT(*) AS cnt
        FROM FACT_SALES
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
    );

    -- Log results to audit table
    INSERT INTO AUDIT.DATA_QUALITY_LOG (check_name, status, error_count, details, run_timestamp)
    SELECT check_name, status, error_count, details, check_time FROM TEMP_DQ_RESULTS;

    result := (SELECT * FROM TEMP_DQ_RESULTS);
    RETURN TABLE(result);
END;
$$;

-- =============================================================================
-- SECTION 3: INCREMENTAL LOAD PROCEDURE WITH ERROR HANDLING
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_INCREMENTAL_LOAD_WITH_AUDIT(
    p_table_name VARCHAR,
    p_batch_size INTEGER DEFAULT 10000
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_rows_processed INTEGER := 0;
    v_start_time TIMESTAMP_NTZ;
    v_error_message VARCHAR;
BEGIN
    v_batch_id := UUID_STRING();
    v_start_time := CURRENT_TIMESTAMP();

    -- Log start
    INSERT INTO STAGING.STG_LOAD_AUDIT (batch_id, table_name, load_start_time, status)
    VALUES (:v_batch_id, :p_table_name, :v_start_time, 'STARTED');

    BEGIN
        -- Perform the load (example for sales)
        IF (p_table_name = 'FACT_SALES') THEN
            CALL SP_LOAD_FACT_SALES();
        END IF;

        -- Update audit with success
        UPDATE STAGING.STG_LOAD_AUDIT
        SET status = 'COMPLETED',
            load_end_time = CURRENT_TIMESTAMP(),
            rows_loaded = :v_rows_processed
        WHERE batch_id = :v_batch_id;

        RETURN 'Load completed: ' || v_batch_id;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := SQLERRM;

            -- Log failure
            UPDATE STAGING.STG_LOAD_AUDIT
            SET status = 'FAILED',
                load_end_time = CURRENT_TIMESTAMP(),
                error_message = :v_error_message
            WHERE batch_id = :v_batch_id;

            RETURN 'Load failed: ' || v_error_message;
    END;
END;
$$;

-- =============================================================================
-- SECTION 4: JAVASCRIPT PROCEDURE EXAMPLE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_GENERATE_REPORT_JS(
    report_type VARCHAR,
    start_date DATE,
    end_date DATE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    // JavaScript procedure for complex report generation
    var result = {
        report_type: REPORT_TYPE,
        start_date: START_DATE,
        end_date: END_DATE,
        generated_at: new Date().toISOString(),
        metrics: {}
    };

    // Execute SQL to get metrics
    var stmt = snowflake.createStatement({
        sqlText: `SELECT
                    COUNT(DISTINCT order_id) as total_orders,
                    SUM(net_amount) as total_revenue
                  FROM CURATED.FACT_SALES f
                  JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
                  WHERE d.full_date BETWEEN :1 AND :2`,
        binds: [START_DATE, END_DATE]
    });

    var rs = stmt.execute();
    if (rs.next()) {
        result.metrics.total_orders = rs.getColumnValue(1);
        result.metrics.total_revenue = rs.getColumnValue(2);
    }

    return result;
$$;

-- Usage: CALL SP_GENERATE_REPORT_JS('SALES', '2024-01-01', '2024-01-31');

-- =============================================================================
-- SECTION 5: CREATE AUDIT LOG TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS AUDIT.DATA_QUALITY_LOG (
    log_id INTEGER AUTOINCREMENT,
    check_name VARCHAR,
    status VARCHAR,
    error_count INTEGER,
    details VARCHAR,
    run_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (log_id)
);

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: SQL vs JavaScript procedures - when to use each?
A1: SQL: Simple logic, set-based operations, better performance
    JavaScript: Complex logic, string manipulation, external API calls

Q2: How do you handle errors in procedures?
A2: Use BEGIN...EXCEPTION...END blocks, SQLERRM for error message,
    log errors to audit tables, return meaningful status.
*/
