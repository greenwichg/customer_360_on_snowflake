/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DATA QUALITY VALIDATION PROCEDURES
================================================================================
Purpose: Automated data quality checks across all pipeline layers
Concepts: Data quality rules, validation procedures, quality metrics logging

Interview Points:
- Data quality should be checked at each pipeline layer
- Automated procedures prevent bad data from propagating downstream
- Quality metrics should be logged for trend analysis and SLA monitoring
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: DATA QUALITY METRICS TABLE
-- =============================================================================

CREATE OR REPLACE TABLE AUDIT.DQ_VALIDATION_LOG (
    validation_id INTEGER AUTOINCREMENT,
    validation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    layer_name VARCHAR(50),
    table_name VARCHAR(200),
    rule_name VARCHAR(200),
    rule_category VARCHAR(50),     -- COMPLETENESS, ACCURACY, CONSISTENCY, TIMELINESS, UNIQUENESS
    records_checked INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    pass_rate DECIMAL(5,2),
    status VARCHAR(20),            -- PASS, FAIL, WARNING
    threshold DECIMAL(5,2),
    details VARCHAR(2000),
    PRIMARY KEY (validation_id)
)
COMMENT = 'Data quality validation results log';

-- =============================================================================
-- SECTION 2: COMPLETENESS CHECKS
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_DQ_CHECK_COMPLETENESS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total INTEGER;
    v_nulls INTEGER;
    v_pass_rate DECIMAL(5,2);
BEGIN
    -- Check 1: Customer email completeness
    SELECT COUNT(*), COUNT(*) - COUNT(email)
    INTO v_total, v_nulls
    FROM CURATED.DIM_CUSTOMER WHERE is_current = TRUE;

    LET v_pass_rate := ROUND((v_total - v_nulls) / NULLIF(v_total, 0) * 100, 2);

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'DIM_CUSTOMER', 'EMAIL_NOT_NULL', 'COMPLETENESS',
         v_total, v_total - v_nulls, v_nulls, v_pass_rate,
         CASE WHEN v_pass_rate >= 95 THEN 'PASS' WHEN v_pass_rate >= 90 THEN 'WARNING' ELSE 'FAIL' END,
         95.00, v_nulls || ' customers missing email');

    -- Check 2: Fact sales required fields
    SELECT COUNT(*),
           COUNT(*) - COUNT(CASE WHEN date_key IS NOT NULL AND order_id IS NOT NULL AND net_amount IS NOT NULL THEN 1 END)
    INTO v_total, v_nulls
    FROM CURATED.FACT_SALES;

    LET v_pass_rate := ROUND((v_total - v_nulls) / NULLIF(v_total, 0) * 100, 2);

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'REQUIRED_FIELDS_NOT_NULL', 'COMPLETENESS',
         v_total, v_total - v_nulls, v_nulls, v_pass_rate,
         CASE WHEN v_pass_rate >= 99 THEN 'PASS' ELSE 'FAIL' END,
         99.00, v_nulls || ' records missing required fields (date_key, order_id, net_amount)');

    -- Check 3: Product name completeness
    SELECT COUNT(*), COUNT(*) - COUNT(product_name)
    INTO v_total, v_nulls
    FROM CURATED.DIM_PRODUCT;

    LET v_pass_rate := ROUND((v_total - v_nulls) / NULLIF(v_total, 0) * 100, 2);

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'DIM_PRODUCT', 'PRODUCT_NAME_NOT_NULL', 'COMPLETENESS',
         v_total, v_total - v_nulls, v_nulls, v_pass_rate,
         CASE WHEN v_pass_rate >= 99 THEN 'PASS' ELSE 'FAIL' END,
         99.00, v_nulls || ' products missing name');

    RETURN 'Completeness checks completed';
END;
$$;

-- =============================================================================
-- SECTION 3: UNIQUENESS CHECKS
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_DQ_CHECK_UNIQUENESS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total INTEGER;
    v_duplicates INTEGER;
BEGIN
    -- Check 1: Unique customer_id for current records
    SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT customer_id)
    INTO v_total, v_duplicates
    FROM CURATED.DIM_CUSTOMER WHERE is_current = TRUE;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'DIM_CUSTOMER', 'UNIQUE_CUSTOMER_ID_CURRENT', 'UNIQUENESS',
         v_total, v_total - v_duplicates, v_duplicates,
         ROUND((v_total - v_duplicates) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_duplicates = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_duplicates || ' duplicate customer_ids in current records');

    -- Check 2: Unique order_id + order_line_id in fact table
    SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT order_id || '-' || order_line_id)
    INTO v_total, v_duplicates
    FROM CURATED.FACT_SALES;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'UNIQUE_ORDER_LINE', 'UNIQUENESS',
         v_total, v_total - v_duplicates, v_duplicates,
         ROUND((v_total - v_duplicates) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_duplicates = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_duplicates || ' duplicate order line records');

    -- Check 3: Unique date_key in date dimension
    SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT date_key)
    INTO v_total, v_duplicates
    FROM CURATED.DIM_DATE;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'DIM_DATE', 'UNIQUE_DATE_KEY', 'UNIQUENESS',
         v_total, v_total - v_duplicates, v_duplicates,
         ROUND((v_total - v_duplicates) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_duplicates = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_duplicates || ' duplicate date keys');

    RETURN 'Uniqueness checks completed';
END;
$$;

-- =============================================================================
-- SECTION 4: ACCURACY / RANGE CHECKS
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_DQ_CHECK_ACCURACY()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total INTEGER;
    v_invalid INTEGER;
BEGIN
    -- Check 1: Net amount should be positive
    SELECT COUNT(*), COUNT(CASE WHEN net_amount < 0 THEN 1 END)
    INTO v_total, v_invalid
    FROM CURATED.FACT_SALES;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'NET_AMOUNT_POSITIVE', 'ACCURACY',
         v_total, v_total - v_invalid, v_invalid,
         ROUND((v_total - v_invalid) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_invalid = 0 THEN 'PASS' ELSE 'WARNING' END,
         99.00, v_invalid || ' records with negative net_amount');

    -- Check 2: Quantity should be reasonable (1-1000)
    SELECT COUNT(*), COUNT(CASE WHEN quantity < 1 OR quantity > 1000 THEN 1 END)
    INTO v_total, v_invalid
    FROM CURATED.FACT_SALES;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'QUANTITY_IN_RANGE', 'ACCURACY',
         v_total, v_total - v_invalid, v_invalid,
         ROUND((v_total - v_invalid) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_invalid = 0 THEN 'PASS' ELSE 'WARNING' END,
         99.00, v_invalid || ' records with quantity outside 1-1000 range');

    -- Check 3: Discount percent should be 0-100
    SELECT COUNT(*), COUNT(CASE WHEN discount_percent < 0 OR discount_percent > 100 THEN 1 END)
    INTO v_total, v_invalid
    FROM CURATED.FACT_SALES;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'DISCOUNT_PCT_IN_RANGE', 'ACCURACY',
         v_total, v_total - v_invalid, v_invalid,
         ROUND((v_total - v_invalid) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_invalid = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_invalid || ' records with discount_percent outside 0-100 range');

    -- Check 4: Email format validation
    SELECT COUNT(*),
           COUNT(CASE WHEN email NOT LIKE '%_@_%.__%' THEN 1 END)
    INTO v_total, v_invalid
    FROM CURATED.DIM_CUSTOMER
    WHERE is_current = TRUE AND email IS NOT NULL;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'DIM_CUSTOMER', 'EMAIL_FORMAT_VALID', 'ACCURACY',
         v_total, v_total - v_invalid, v_invalid,
         ROUND((v_total - v_invalid) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_invalid = 0 THEN 'PASS' ELSE 'WARNING' END,
         98.00, v_invalid || ' customers with invalid email format');

    RETURN 'Accuracy checks completed';
END;
$$;

-- =============================================================================
-- SECTION 5: CONSISTENCY / REFERENTIAL INTEGRITY CHECKS
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_DQ_CHECK_CONSISTENCY()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total INTEGER;
    v_orphans INTEGER;
BEGIN
    -- Check 1: Fact sales → Dim customer referential integrity
    SELECT COUNT(*), COUNT(CASE WHEN c.customer_key IS NULL THEN 1 END)
    INTO v_total, v_orphans
    FROM CURATED.FACT_SALES f
    LEFT JOIN CURATED.DIM_CUSTOMER c ON f.customer_key = c.customer_key
    WHERE f.customer_key IS NOT NULL;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'FK_CUSTOMER_INTEGRITY', 'CONSISTENCY',
         v_total, v_total - v_orphans, v_orphans,
         ROUND((v_total - v_orphans) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_orphans = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_orphans || ' orphan customer_key references in FACT_SALES');

    -- Check 2: Fact sales → Dim product referential integrity
    SELECT COUNT(*), COUNT(CASE WHEN p.product_key IS NULL THEN 1 END)
    INTO v_total, v_orphans
    FROM CURATED.FACT_SALES f
    LEFT JOIN CURATED.DIM_PRODUCT p ON f.product_key = p.product_key
    WHERE f.product_key IS NOT NULL;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'FK_PRODUCT_INTEGRITY', 'CONSISTENCY',
         v_total, v_total - v_orphans, v_orphans,
         ROUND((v_total - v_orphans) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_orphans = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_orphans || ' orphan product_key references in FACT_SALES');

    -- Check 3: Fact sales → Dim date referential integrity
    SELECT COUNT(*), COUNT(CASE WHEN d.date_key IS NULL THEN 1 END)
    INTO v_total, v_orphans
    FROM CURATED.FACT_SALES f
    LEFT JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'FK_DATE_INTEGRITY', 'CONSISTENCY',
         v_total, v_total - v_orphans, v_orphans,
         ROUND((v_total - v_orphans) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_orphans = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_orphans || ' orphan date_key references in FACT_SALES');

    -- Check 4: SCD Type 2 consistency - exactly one current record per customer
    SELECT COUNT(DISTINCT customer_id),
           COUNT(CASE WHEN cnt > 1 THEN 1 END)
    INTO v_total, v_orphans
    FROM (
        SELECT customer_id, COUNT(*) AS cnt
        FROM CURATED.DIM_CUSTOMER WHERE is_current = TRUE
        GROUP BY customer_id
    );

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'DIM_CUSTOMER', 'SCD2_ONE_CURRENT_PER_KEY', 'CONSISTENCY',
         v_total, v_total - v_orphans, v_orphans,
         ROUND((v_total - v_orphans) / NULLIF(v_total, 0) * 100, 2),
         CASE WHEN v_orphans = 0 THEN 'PASS' ELSE 'FAIL' END,
         100.00, v_orphans || ' customers with multiple current records');

    RETURN 'Consistency checks completed';
END;
$$;

-- =============================================================================
-- SECTION 6: TIMELINESS CHECKS
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_DQ_CHECK_TIMELINESS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_stale_count INTEGER;
    v_max_load_ts TIMESTAMP_NTZ;
BEGIN
    -- Check 1: Fact sales should have data loaded within last 24 hours
    SELECT MAX(load_timestamp), COUNT(CASE WHEN load_timestamp < DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN NULL ELSE 1 END)
    INTO v_max_load_ts, v_stale_count
    FROM CURATED.FACT_SALES;

    INSERT INTO AUDIT.DQ_VALIDATION_LOG
        (layer_name, table_name, rule_name, rule_category,
         records_checked, records_passed, records_failed, pass_rate,
         status, threshold, details)
    VALUES
        ('CURATED', 'FACT_SALES', 'DATA_FRESHNESS_24H', 'TIMELINESS',
         1, CASE WHEN v_max_load_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END,
         CASE WHEN v_max_load_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN 0 ELSE 1 END,
         CASE WHEN v_max_load_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN 100 ELSE 0 END,
         CASE WHEN v_max_load_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN 'PASS' ELSE 'FAIL' END,
         100.00,
         'Last load timestamp: ' || COALESCE(TO_CHAR(v_max_load_ts), 'NO DATA'));

    RETURN 'Timeliness checks completed';
END;
$$;

-- =============================================================================
-- SECTION 7: MASTER VALIDATION PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_ALL_DQ_CHECKS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_fail_count INTEGER;
BEGIN
    CALL SP_DQ_CHECK_COMPLETENESS();
    CALL SP_DQ_CHECK_UNIQUENESS();
    CALL SP_DQ_CHECK_ACCURACY();
    CALL SP_DQ_CHECK_CONSISTENCY();
    CALL SP_DQ_CHECK_TIMELINESS();

    -- Count failures from this run
    SELECT COUNT(*)
    INTO v_fail_count
    FROM AUDIT.DQ_VALIDATION_LOG
    WHERE validation_timestamp >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    AND status = 'FAIL';

    RETURN 'All DQ checks completed. Failures: ' || v_fail_count;
END;
$$;

-- =============================================================================
-- SECTION 8: DQ REPORTING VIEW
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.VW_DQ_LATEST_RESULTS AS
SELECT
    validation_timestamp,
    layer_name,
    table_name,
    rule_name,
    rule_category,
    records_checked,
    records_failed,
    pass_rate,
    status,
    threshold,
    details
FROM AUDIT.DQ_VALIDATION_LOG
QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name, rule_name ORDER BY validation_timestamp DESC) = 1
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
    table_name, rule_name;

-- DQ trend summary
CREATE OR REPLACE VIEW AUDIT.VW_DQ_DAILY_SUMMARY AS
SELECT
    DATE(validation_timestamp) AS check_date,
    rule_category,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS overall_pass_rate
FROM AUDIT.DQ_VALIDATION_LOG
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- =============================================================================
-- SECTION 9: SCHEDULE DQ CHECKS
-- =============================================================================

-- Run DQ checks daily after ETL completes
CREATE OR REPLACE TASK TASK_DQ_VALIDATION
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- 6 AM ET daily
    COMMENT = 'Daily data quality validation checks'
AS
    CALL SP_RUN_ALL_DQ_CHECKS();

-- Enable the task
-- ALTER TASK TASK_DQ_VALIDATION RESUME;

-- =============================================================================
-- SECTION 10: GRANT PRIVILEGES
-- =============================================================================

GRANT SELECT ON TABLE AUDIT.DQ_VALIDATION_LOG TO ROLE RETAIL_ADMIN;
GRANT SELECT ON VIEW AUDIT.VW_DQ_LATEST_RESULTS TO ROLE RETAIL_ADMIN;
GRANT SELECT ON VIEW AUDIT.VW_DQ_DAILY_SUMMARY TO ROLE RETAIL_ADMIN;
GRANT SELECT ON VIEW AUDIT.VW_DQ_LATEST_RESULTS TO ROLE RETAIL_ANALYST;
GRANT SELECT ON VIEW AUDIT.VW_DQ_DAILY_SUMMARY TO ROLE RETAIL_ANALYST;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: What are the dimensions of data quality?
A1: The six key dimensions:
    - COMPLETENESS: Are all expected fields populated?
    - ACCURACY: Are values correct and within expected ranges?
    - CONSISTENCY: Are relationships and business rules maintained?
    - TIMELINESS: Is data fresh and loaded on schedule?
    - UNIQUENESS: Are there no unintended duplicates?
    - VALIDITY: Does data conform to expected formats?

Q2: How do you handle data quality failures in a pipeline?
A2: Options include:
    - Quarantine: Move bad records to an error table for review
    - Alert: Send notification but continue processing
    - Block: Stop the pipeline until issues are resolved
    - Default: Replace invalid values with defaults
    The approach depends on the severity and business impact.

Q3: Why log DQ metrics over time?
A3: Trend analysis reveals:
    - Degrading source data quality
    - Impact of system changes on data integrity
    - SLA compliance for data freshness
    - Patterns in data quality issues (e.g., weekend vs weekday)
*/
