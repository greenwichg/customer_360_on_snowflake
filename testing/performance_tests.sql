/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - PERFORMANCE TESTS
================================================================================
Purpose: Benchmark and validate query performance
Concepts: Query profiling, execution benchmarks, warehouse sizing tests

Interview Points:
- Performance testing validates warehouse sizing decisions
- Query profiling identifies optimization opportunities
- Benchmark queries simulate real workload patterns
- Results guide clustering and optimization decisions
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: BENCHMARK QUERY SET
-- =============================================================================

-- Store benchmark results
CREATE OR REPLACE TABLE AUDIT.PERFORMANCE_BENCHMARK (
    test_id INTEGER AUTOINCREMENT,
    test_name VARCHAR(200),
    test_category VARCHAR(50),
    warehouse_size VARCHAR(20),
    execution_time_ms INTEGER,
    rows_returned INTEGER,
    bytes_scanned BIGINT,
    partitions_scanned INTEGER,
    partitions_total INTEGER,
    pruning_pct DECIMAL(5,2),
    bytes_spilled_local BIGINT,
    bytes_spilled_remote BIGINT,
    test_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (test_id)
)
COMMENT = 'Performance benchmark results';

-- =============================================================================
-- SECTION 2: PERFORMANCE TEST PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_PERFORMANCE_TESTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_query_id VARCHAR;
    v_exec_time INTEGER;
    v_rows INTEGER;
    v_bytes BIGINT;
    v_partitions_scanned INTEGER;
    v_partitions_total INTEGER;
BEGIN
    -- -------------------------------------------
    -- Test 1: Simple date range scan
    -- -------------------------------------------
    SELECT COUNT(*), SUM(net_amount)
    FROM CURATED.FACT_SALES
    WHERE date_key BETWEEN 20240101 AND 20240131;

    SELECT LAST_QUERY_ID() INTO v_query_id;

    INSERT INTO AUDIT.PERFORMANCE_BENCHMARK
        (test_name, test_category, warehouse_size, execution_time_ms,
         rows_returned, bytes_scanned, partitions_scanned, partitions_total,
         pruning_pct, bytes_spilled_local, bytes_spilled_remote)
    SELECT
        'Date range scan (1 month)',
        'SCAN',
        WAREHOUSE_SIZE,
        EXECUTION_TIME,
        ROWS_PRODUCED,
        BYTES_SCANNED,
        PARTITIONS_SCANNED,
        PARTITIONS_TOTAL,
        ROUND(1 - (PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0)), 4) * 100,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE QUERY_ID = v_query_id;

    -- -------------------------------------------
    -- Test 2: Multi-table join with aggregation
    -- -------------------------------------------
    SELECT
        st.region,
        p.category,
        COUNT(DISTINCT f.order_id) AS orders,
        SUM(f.net_amount) AS revenue
    FROM CURATED.FACT_SALES f
    JOIN CURATED.DIM_DATE d ON f.date_key = d.date_key
    JOIN CURATED.DIM_STORE st ON f.store_key = st.store_key
    JOIN CURATED.DIM_PRODUCT p ON f.product_key = p.product_key
    WHERE d.year_number = 2024
    GROUP BY 1, 2
    ORDER BY revenue DESC;

    SELECT LAST_QUERY_ID() INTO v_query_id;

    INSERT INTO AUDIT.PERFORMANCE_BENCHMARK
        (test_name, test_category, warehouse_size, execution_time_ms,
         rows_returned, bytes_scanned, partitions_scanned, partitions_total,
         pruning_pct, bytes_spilled_local, bytes_spilled_remote)
    SELECT
        'Multi-join aggregation (year)',
        'JOIN_AGG',
        WAREHOUSE_SIZE,
        EXECUTION_TIME,
        ROWS_PRODUCED,
        BYTES_SCANNED,
        PARTITIONS_SCANNED,
        PARTITIONS_TOTAL,
        ROUND(1 - (PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0)), 4) * 100,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE QUERY_ID = v_query_id;

    -- -------------------------------------------
    -- Test 3: Point lookup (customer by ID)
    -- -------------------------------------------
    SELECT *
    FROM CURATED.DIM_CUSTOMER
    WHERE customer_id = 'CUST-10001' AND is_current = TRUE;

    SELECT LAST_QUERY_ID() INTO v_query_id;

    INSERT INTO AUDIT.PERFORMANCE_BENCHMARK
        (test_name, test_category, warehouse_size, execution_time_ms,
         rows_returned, bytes_scanned, partitions_scanned, partitions_total,
         pruning_pct, bytes_spilled_local, bytes_spilled_remote)
    SELECT
        'Point lookup (customer_id)',
        'LOOKUP',
        WAREHOUSE_SIZE,
        EXECUTION_TIME,
        ROWS_PRODUCED,
        BYTES_SCANNED,
        PARTITIONS_SCANNED,
        PARTITIONS_TOTAL,
        ROUND(1 - (PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0)), 4) * 100,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE QUERY_ID = v_query_id;

    -- -------------------------------------------
    -- Test 4: Window function (ranking)
    -- -------------------------------------------
    SELECT
        customer_key,
        order_id,
        net_amount,
        ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY net_amount DESC) AS purchase_rank,
        SUM(net_amount) OVER (PARTITION BY customer_key ORDER BY date_key) AS running_total
    FROM CURATED.FACT_SALES
    WHERE date_key BETWEEN 20240101 AND 20241231
    LIMIT 10000;

    SELECT LAST_QUERY_ID() INTO v_query_id;

    INSERT INTO AUDIT.PERFORMANCE_BENCHMARK
        (test_name, test_category, warehouse_size, execution_time_ms,
         rows_returned, bytes_scanned, partitions_scanned, partitions_total,
         pruning_pct, bytes_spilled_local, bytes_spilled_remote)
    SELECT
        'Window function (ranking + running total)',
        'WINDOW',
        WAREHOUSE_SIZE,
        EXECUTION_TIME,
        ROWS_PRODUCED,
        BYTES_SCANNED,
        PARTITIONS_SCANNED,
        PARTITIONS_TOTAL,
        ROUND(1 - (PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0)), 4) * 100,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE QUERY_ID = v_query_id;

    RETURN 'Performance tests completed. Check AUDIT.PERFORMANCE_BENCHMARK for results.';
END;
$$;

-- =============================================================================
-- SECTION 3: WAREHOUSE SIZE COMPARISON TEST
-- =============================================================================

-- Compare performance across different warehouse sizes
-- Run the same query on different warehouses to determine optimal sizing

CREATE OR REPLACE PROCEDURE SP_WAREHOUSE_SIZE_BENCHMARK(p_test_query VARCHAR)
RETURNS TABLE (warehouse_size VARCHAR, execution_ms INTEGER, credits_used DECIMAL(10,4))
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    -- Note: This procedure demonstrates the concept
    -- In practice, you would run on each warehouse size and compare
    result := (
        SELECT
            WAREHOUSE_SIZE AS warehouse_size,
            EXECUTION_TIME AS execution_ms,
            ROUND(EXECUTION_TIME / 1000 / 3600 *
                CASE WAREHOUSE_SIZE
                    WHEN 'X-Small' THEN 1
                    WHEN 'Small' THEN 2
                    WHEN 'Medium' THEN 4
                    WHEN 'Large' THEN 8
                    WHEN 'X-Large' THEN 16
                END, 4) AS credits_used
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE QUERY_TEXT LIKE '%FACT_SALES%'
        AND START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
        ORDER BY START_TIME DESC
        LIMIT 10
    );
    RETURN TABLE(result);
END;
$$;

-- =============================================================================
-- SECTION 4: PERFORMANCE RESULTS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.VW_PERFORMANCE_RESULTS AS
SELECT
    test_name,
    test_category,
    warehouse_size,
    execution_time_ms,
    rows_returned,
    ROUND(bytes_scanned / (1024*1024), 2) AS mb_scanned,
    partitions_scanned,
    partitions_total,
    pruning_pct,
    ROUND(bytes_spilled_local / (1024*1024), 2) AS mb_spilled_local,
    ROUND(bytes_spilled_remote / (1024*1024), 2) AS mb_spilled_remote,
    CASE
        WHEN bytes_spilled_remote > 0 THEN 'NEEDS_LARGER_WAREHOUSE'
        WHEN pruning_pct < 50 THEN 'NEEDS_CLUSTERING'
        WHEN execution_time_ms > 60000 THEN 'NEEDS_OPTIMIZATION'
        ELSE 'OK'
    END AS recommendation,
    test_timestamp
FROM AUDIT.PERFORMANCE_BENCHMARK
ORDER BY test_timestamp DESC;

-- =============================================================================
-- SECTION 5: RUN TESTS
-- =============================================================================

-- Execute: CALL SP_RUN_PERFORMANCE_TESTS();
-- Review: SELECT * FROM AUDIT.VW_PERFORMANCE_RESULTS;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How do you benchmark Snowflake query performance?
A1: - Run standardized test queries that represent real workloads
    - Capture execution time, bytes scanned, pruning ratio, spilling
    - Compare across warehouse sizes to find optimal cost/performance
    - Track over time to detect performance regressions

Q2: What indicates a query needs optimization?
A2: Red flags:
    - bytes_spilled_remote > 0 (warehouse too small)
    - pruning_pct < 50% (need clustering keys)
    - High execution_time relative to rows returned
    - Large gap between partitions_scanned vs partitions_total
    - Queuing time > 0 (warehouse overloaded)

Q3: How do you choose the right warehouse size?
A3: - Start with X-Small, scale up until spilling stops
    - Run the same benchmark on each size
    - Compare cost (credits) vs performance (time)
    - A Medium that finishes in 1 min may cost less than X-Small taking 10 min
    - Check: 2x size = ~2x speed for scan-heavy queries
*/
