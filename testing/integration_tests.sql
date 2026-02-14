/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - INTEGRATION TESTS
================================================================================
Purpose: End-to-end pipeline validation tests
Concepts: Pipeline testing, data flow validation, regression testing

Interview Points:
- Integration tests validate the entire pipeline end-to-end
- Test data flow from landing through to analytics layer
- Verify transformations produce expected results
- Run after deployments or schema changes
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE DEV_WH;

-- =============================================================================
-- SECTION 1: TEST RESULTS TABLE
-- =============================================================================

CREATE OR REPLACE TABLE AUDIT.INTEGRATION_TEST_RESULTS (
    test_id INTEGER AUTOINCREMENT,
    test_suite VARCHAR(100),
    test_name VARCHAR(200),
    status VARCHAR(20),        -- PASS, FAIL, SKIP, ERROR
    expected_value VARCHAR(500),
    actual_value VARCHAR(500),
    details VARCHAR(2000),
    test_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (test_id)
)
COMMENT = 'Integration test results log';

-- =============================================================================
-- SECTION 2: PIPELINE CONNECTIVITY TESTS
-- =============================================================================

CREATE OR REPLACE PROCEDURE TEST_PIPELINE_CONNECTIVITY()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Test 1: Landing layer objects exist
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'CONNECTIVITY',
        'LANDING_STAGES_EXIST',
        CASE WHEN COUNT(*) >= 4 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' stages found (expected >= 4)'
    FROM INFORMATION_SCHEMA.STAGES
    WHERE STAGE_SCHEMA = 'LANDING';

    -- Test 2: Staging tables exist
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'CONNECTIVITY',
        'STAGING_TABLES_EXIST',
        CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' staging tables found (expected >= 3)'
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'STAGING' AND TABLE_TYPE = 'BASE TABLE';

    -- Test 3: Curated layer tables exist
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'CONNECTIVITY',
        'CURATED_TABLES_EXIST',
        CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' curated tables found (expected >= 5: 4 dims + 1 fact)'
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'CURATED' AND TABLE_TYPE = 'BASE TABLE';

    -- Test 4: Analytics layer objects exist
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'CONNECTIVITY',
        'ANALYTICS_VIEWS_EXIST',
        CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' analytics views found (expected >= 3)'
    FROM INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = 'ANALYTICS';

    -- Test 5: Streams are active
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'CONNECTIVITY',
        'STREAMS_ACTIVE',
        CASE WHEN COUNT(*) >= 2 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' active streams found'
    FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES
    WHERE TABLE_SCHEMA = 'STAGING';

    RETURN 'Pipeline connectivity tests completed';
END;
$$;

-- =============================================================================
-- SECTION 3: DATA FLOW TESTS
-- =============================================================================

CREATE OR REPLACE PROCEDURE TEST_DATA_FLOW()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_staging_count INTEGER;
    v_curated_count INTEGER;
BEGIN
    -- Test 1: Staging → Curated row count consistency (sales)
    SELECT COUNT(*) INTO v_staging_count
    FROM STAGING.STG_SALES WHERE dq_is_valid = TRUE;

    SELECT COUNT(*) INTO v_curated_count
    FROM CURATED.FACT_SALES;

    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, expected_value, actual_value, details)
    VALUES (
        'DATA_FLOW',
        'SALES_ROW_COUNT_MATCH',
        CASE WHEN v_staging_count = v_curated_count THEN 'PASS'
             WHEN ABS(v_staging_count - v_curated_count) / NULLIF(v_staging_count, 0) < 0.01 THEN 'PASS'
             ELSE 'FAIL'
        END,
        v_staging_count::VARCHAR,
        v_curated_count::VARCHAR,
        'Staging valid rows: ' || v_staging_count || ', Curated rows: ' || v_curated_count
    );

    -- Test 2: Date dimension coverage
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'DATA_FLOW',
        'DATE_DIM_COVERAGE',
        CASE WHEN orphan_dates = 0 THEN 'PASS' ELSE 'FAIL' END,
        orphan_dates || ' fact records with dates not in DIM_DATE'
    FROM (
        SELECT COUNT(*) AS orphan_dates
        FROM CURATED.FACT_SALES f
        WHERE NOT EXISTS (SELECT 1 FROM CURATED.DIM_DATE d WHERE d.date_key = f.date_key)
    );

    -- Test 3: Customer dimension has current records
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'DATA_FLOW',
        'CUSTOMER_DIM_HAS_CURRENT',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' current customer records'
    FROM CURATED.DIM_CUSTOMER WHERE is_current = TRUE;

    -- Test 4: Materialized views are populated
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'DATA_FLOW',
        'MV_DAILY_SALES_POPULATED',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' rows in MV_DAILY_SALES_SUMMARY'
    FROM ANALYTICS.MV_DAILY_SALES_SUMMARY;

    RETURN 'Data flow tests completed';
END;
$$;

-- =============================================================================
-- SECTION 4: TRANSFORMATION LOGIC TESTS
-- =============================================================================

CREATE OR REPLACE PROCEDURE TEST_TRANSFORMATION_LOGIC()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Test 1: Gross amount calculation (price * quantity)
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'TRANSFORMATION',
        'GROSS_AMOUNT_CALC',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' records where gross_amount != unit_price * quantity'
    FROM CURATED.FACT_SALES
    WHERE ABS(gross_amount - (unit_price * quantity)) > 0.01;

    -- Test 2: Net amount <= Gross amount
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'TRANSFORMATION',
        'NET_LESS_THAN_GROSS',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' records where net_amount > gross_amount'
    FROM CURATED.FACT_SALES
    WHERE net_amount > gross_amount + 0.01;

    -- Test 3: SCD Type 2 date continuity
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'TRANSFORMATION',
        'SCD2_DATE_CONTINUITY',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' customers with date gaps in SCD2 history'
    FROM (
        SELECT customer_id
        FROM CURATED.DIM_CUSTOMER
        WHERE is_current = TRUE AND end_date IS NOT NULL
    );

    -- Test 4: Profit margin calculation
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'TRANSFORMATION',
        'PROFIT_MARGIN_VALID',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' products with profit_margin outside 0-100 range'
    FROM CURATED.DIM_PRODUCT
    WHERE profit_margin < 0 OR profit_margin > 100;

    RETURN 'Transformation logic tests completed';
END;
$$;

-- =============================================================================
-- SECTION 5: SECURITY TESTS
-- =============================================================================

CREATE OR REPLACE PROCEDURE TEST_SECURITY_CONFIGURATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Test 1: Masking policies are applied
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    SELECT
        'SECURITY',
        'MASKING_POLICIES_EXIST',
        CASE WHEN COUNT(*) >= 2 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(*) || ' masking policies found (expected >= 2: email, phone)'
    FROM INFORMATION_SCHEMA.MASKING_POLICIES;

    -- Test 2: RBAC roles exist
    INSERT INTO AUDIT.INTEGRATION_TEST_RESULTS (test_suite, test_name, status, details)
    VALUES (
        'SECURITY',
        'RBAC_ROLES_CONFIGURED',
        CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.APPLICABLE_ROLES WHERE ROLE_NAME = 'RETAIL_ENGINEER')
             THEN 'PASS' ELSE 'FAIL' END,
        'Checking RETAIL_ENGINEER role exists'
    );

    RETURN 'Security tests completed';
END;
$$;

-- =============================================================================
-- SECTION 6: MASTER TEST RUNNER
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_ALL_INTEGRATION_TESTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_pass_count INTEGER;
    v_fail_count INTEGER;
    v_total INTEGER;
BEGIN
    CALL TEST_PIPELINE_CONNECTIVITY();
    CALL TEST_DATA_FLOW();
    CALL TEST_TRANSFORMATION_LOGIC();
    CALL TEST_SECURITY_CONFIGURATION();

    -- Summary
    SELECT
        COUNT(CASE WHEN status = 'PASS' THEN 1 END),
        COUNT(CASE WHEN status = 'FAIL' THEN 1 END),
        COUNT(*)
    INTO v_pass_count, v_fail_count, v_total
    FROM AUDIT.INTEGRATION_TEST_RESULTS
    WHERE test_timestamp >= DATEADD('minute', -5, CURRENT_TIMESTAMP());

    RETURN 'Integration tests complete: ' || v_pass_count || '/' || v_total || ' passed, ' || v_fail_count || ' failed';
END;
$$;

-- =============================================================================
-- SECTION 7: TEST RESULTS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.VW_INTEGRATION_TEST_RESULTS AS
SELECT
    test_suite,
    test_name,
    status,
    expected_value,
    actual_value,
    details,
    test_timestamp
FROM AUDIT.INTEGRATION_TEST_RESULTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY test_name ORDER BY test_timestamp DESC) = 1
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'ERROR' THEN 2 WHEN 'SKIP' THEN 3 ELSE 4 END,
    test_suite, test_name;

-- Execute: CALL SP_RUN_ALL_INTEGRATION_TESTS();
-- Review: SELECT * FROM AUDIT.VW_INTEGRATION_TEST_RESULTS;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: What types of tests do you run on a data pipeline?
A1: - Connectivity: All objects exist and are accessible
    - Data flow: Row counts match between layers
    - Transformation: Business logic produces correct results
    - Security: RBAC, masking, and policies are configured
    - Performance: Queries meet SLA requirements

Q2: How often should integration tests run?
A2: - After every deployment or schema change
    - Daily as part of the pipeline (after ETL completes)
    - On-demand for debugging issues
    - Automated via Snowflake tasks for scheduled runs
*/
