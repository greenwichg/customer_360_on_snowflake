/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DATA VALIDATION
================================================================================
Purpose: Validate data quality during staging layer processing
Concepts: Data quality rules, validation procedures, error tracking
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA STAGING;

-- =============================================================================
-- SECTION 1: VALIDATION RULES FOR SALES
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_VALIDATE_SALES()
RETURNS TABLE (rule_name VARCHAR, status VARCHAR, failed_count INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    CREATE OR REPLACE TEMP TABLE validation_results (
        rule_name VARCHAR,
        status VARCHAR,
        failed_count INTEGER
    );

    -- Rule 1: Order ID not null
    INSERT INTO validation_results
    SELECT 'ORDER_ID_NOT_NULL',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*)
    FROM STG_SALES WHERE order_id IS NULL;

    -- Rule 2: Quantity must be positive
    INSERT INTO validation_results
    SELECT 'QUANTITY_POSITIVE',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*)
    FROM STG_SALES WHERE quantity <= 0;

    -- Rule 3: Total amount calculation
    INSERT INTO validation_results
    SELECT 'AMOUNT_CALCULATION',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*)
    FROM STG_SALES
    WHERE ABS(total_amount - (unit_price * quantity * (1 - discount_percent/100))) > 0.01;

    -- Rule 4: Valid payment method
    INSERT INTO validation_results
    SELECT 'VALID_PAYMENT_METHOD',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*)
    FROM STG_SALES
    WHERE payment_method NOT IN ('CREDIT_CARD', 'DEBIT_CARD', 'CASH', 'PAYPAL');

    -- Mark invalid records
    UPDATE STG_SALES
    SET dq_is_valid = FALSE,
        dq_error_details = 'Failed validation'
    WHERE order_id IS NULL
       OR quantity <= 0
       OR ABS(total_amount - (unit_price * quantity * (1 - discount_percent/100))) > 0.01;

    result := (SELECT * FROM validation_results);
    RETURN TABLE(result);
END;
$$;

-- =============================================================================
-- SECTION 2: VALIDATION RULES FOR CUSTOMERS
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_VALIDATE_CUSTOMERS()
RETURNS TABLE (rule_name VARCHAR, status VARCHAR, failed_count INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    CREATE OR REPLACE TEMP TABLE validation_results (
        rule_name VARCHAR, status VARCHAR, failed_count INTEGER
    );

    -- Rule 1: Customer ID not null
    INSERT INTO validation_results
    SELECT 'CUSTOMER_ID_NOT_NULL',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*)
    FROM STG_CUSTOMERS WHERE customer_id IS NULL;

    -- Rule 2: Valid email format
    INSERT INTO validation_results
    SELECT 'VALID_EMAIL_FORMAT',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*)
    FROM STG_CUSTOMERS
    WHERE email IS NOT NULL
    AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');

    -- Rule 3: Registration date not in future
    INSERT INTO validation_results
    SELECT 'REGISTRATION_NOT_FUTURE',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*)
    FROM STG_CUSTOMERS WHERE registration_date > CURRENT_DATE();

    -- Mark invalid records
    UPDATE STG_CUSTOMERS
    SET dq_is_valid = FALSE,
        dq_error_details = 'Failed validation'
    WHERE customer_id IS NULL
       OR registration_date > CURRENT_DATE();

    result := (SELECT * FROM validation_results);
    RETURN TABLE(result);
END;
$$;

-- =============================================================================
-- SECTION 3: RUN ALL VALIDATIONS
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_ALL_VALIDATIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL SP_VALIDATE_SALES();
    CALL SP_VALIDATE_CUSTOMERS();
    RETURN 'All validations completed';
END;
$$;
