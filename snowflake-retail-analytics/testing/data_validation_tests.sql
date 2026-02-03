/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DATA VALIDATION TESTS
================================================================================
Purpose: Test data quality and pipeline integrity
Concepts: Automated testing, assertions, data reconciliation
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE DEV_WH;

-- =============================================================================
-- SECTION 1: DIMENSIONAL MODEL INTEGRITY TESTS
-- =============================================================================

-- Test: All fact records have valid dimension keys
CREATE OR REPLACE PROCEDURE TEST_FK_INTEGRITY()
RETURNS TABLE (test_name VARCHAR, status VARCHAR, details VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    CREATE OR REPLACE TEMP TABLE test_results (
        test_name VARCHAR, status VARCHAR, details VARCHAR
    );

    -- Test customer FK
    INSERT INTO test_results
    SELECT 'FACT_SALES_CUSTOMER_FK',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*) || ' orphan records'
    FROM CURATED.FACT_SALES f
    WHERE f.customer_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM CURATED.DIM_CUSTOMER c WHERE c.customer_key = f.customer_key);

    -- Test product FK
    INSERT INTO test_results
    SELECT 'FACT_SALES_PRODUCT_FK',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*) || ' orphan records'
    FROM CURATED.FACT_SALES f
    WHERE f.product_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM CURATED.DIM_PRODUCT p WHERE p.product_key = f.product_key);

    -- Test date FK
    INSERT INTO test_results
    SELECT 'FACT_SALES_DATE_FK',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*) || ' orphan records'
    FROM CURATED.FACT_SALES f
    WHERE NOT EXISTS (SELECT 1 FROM CURATED.DIM_DATE d WHERE d.date_key = f.date_key);

    result := (SELECT * FROM test_results);
    RETURN TABLE(result);
END;
$$;

-- =============================================================================
-- SECTION 2: DATA RECONCILIATION TESTS
-- =============================================================================

-- Test: Row counts match between layers
CREATE OR REPLACE PROCEDURE TEST_ROW_COUNT_RECONCILIATION()
RETURNS TABLE (layer VARCHAR, table_name VARCHAR, row_count INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    result := (
        SELECT 'STAGING' AS layer, 'STG_SALES' AS table_name, COUNT(*) AS row_count
        FROM STAGING.STG_SALES WHERE dq_is_valid = TRUE
        UNION ALL
        SELECT 'CURATED', 'FACT_SALES', COUNT(*)
        FROM CURATED.FACT_SALES
        UNION ALL
        SELECT 'STAGING', 'STG_CUSTOMERS', COUNT(*)
        FROM STAGING.STG_CUSTOMERS WHERE dq_is_valid = TRUE
        UNION ALL
        SELECT 'CURATED', 'DIM_CUSTOMER (current)', COUNT(*)
        FROM CURATED.DIM_CUSTOMER WHERE is_current = TRUE
    );
    RETURN TABLE(result);
END;
$$;

-- =============================================================================
-- SECTION 3: BUSINESS LOGIC TESTS
-- =============================================================================

-- Test: SCD Type 2 integrity
CREATE OR REPLACE PROCEDURE TEST_SCD2_INTEGRITY()
RETURNS TABLE (test_name VARCHAR, status VARCHAR, details VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    CREATE OR REPLACE TEMP TABLE test_results (
        test_name VARCHAR, status VARCHAR, details VARCHAR
    );

    -- Each customer should have exactly one current record
    INSERT INTO test_results
    SELECT 'ONE_CURRENT_PER_CUSTOMER',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*) || ' customers with multiple current records'
    FROM (
        SELECT customer_id, COUNT(*) AS cnt
        FROM CURATED.DIM_CUSTOMER
        WHERE is_current = TRUE
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    );

    -- End date should be NULL for current records
    INSERT INTO test_results
    SELECT 'CURRENT_END_DATE_NULL',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*) || ' current records with non-null end_date'
    FROM CURATED.DIM_CUSTOMER
    WHERE is_current = TRUE AND end_date IS NOT NULL;

    result := (SELECT * FROM test_results);
    RETURN TABLE(result);
END;
$$;

-- =============================================================================
-- SECTION 4: RUN ALL TESTS
-- =============================================================================

CREATE OR REPLACE PROCEDURE RUN_ALL_TESTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL TEST_FK_INTEGRITY();
    CALL TEST_ROW_COUNT_RECONCILIATION();
    CALL TEST_SCD2_INTEGRITY();
    RETURN 'All tests completed - check results';
END;
$$;

-- Run: CALL RUN_ALL_TESTS();
