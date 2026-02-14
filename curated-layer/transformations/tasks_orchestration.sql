/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - TASKS ORCHESTRATION
================================================================================
Purpose: Create scheduled tasks with dependencies for ETL pipeline
Concepts: Task DAG, scheduling, error handling, task dependencies

Interview Points:
- Tasks are scheduled SQL execution
- Support for CRON schedules and interval-based
- Task trees (DAG) for dependency management
- Serverless tasks available for auto-scaling
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: ROOT TASK (Scheduler)
-- =============================================================================
/*
Root task runs on schedule and triggers child tasks.
Uses a simple SELECT as placeholder - children do the work.
*/

CREATE OR REPLACE TASK TASK_ETL_ROOT
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour at minute 0
    COMMENT = 'Root task - triggers ETL pipeline every hour'
AS
    SELECT CURRENT_TIMESTAMP() AS etl_start_time;

-- =============================================================================
-- SECTION 2: DIMENSION LOADING TASKS (Level 1)
-- =============================================================================

-- Load Customer Dimension (SCD Type 2)
CREATE OR REPLACE TASK TASK_LOAD_DIM_CUSTOMER
    WAREHOUSE = TRANSFORM_WH
    AFTER TASK_ETL_ROOT
    COMMENT = 'Load customer dimension with SCD Type 2'
AS
    CALL SP_LOAD_DIM_CUSTOMER();

-- Load Product Dimension (SCD Type 1)
CREATE OR REPLACE TASK TASK_LOAD_DIM_PRODUCT
    WAREHOUSE = TRANSFORM_WH
    AFTER TASK_ETL_ROOT
    COMMENT = 'Load product dimension with SCD Type 1'
AS
    MERGE INTO DIM_PRODUCT target
    USING (
        SELECT * FROM STAGING.STG_PRODUCTS_STREAM
        WHERE METADATA$ACTION = 'INSERT' AND dq_is_valid = TRUE
    ) source
    ON target.product_id = source.product_id
    WHEN MATCHED THEN UPDATE SET
        target.product_name = source.product_name,
        target.category = source.category,
        target.subcategory = source.subcategory,
        target.brand = source.brand,
        target.unit_cost = source.unit_cost,
        target.unit_price = source.unit_price,
        target.profit_margin = ROUND((source.unit_price - source.unit_cost) / source.unit_price * 100, 2),
        target.weight_kg = source.weight_kg,
        target.is_active = source.is_active,
        target.attributes = source.attributes,
        target.updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        product_id, product_name, category, subcategory, brand,
        unit_cost, unit_price, profit_margin, weight_kg, is_active,
        launch_date, attributes
    ) VALUES (
        source.product_id, source.product_name, source.category,
        source.subcategory, source.brand, source.unit_cost, source.unit_price,
        ROUND((source.unit_price - source.unit_cost) / source.unit_price * 100, 2),
        source.weight_kg, source.is_active, source.launch_date, source.attributes
    );

-- Load Store Dimension
CREATE OR REPLACE TASK TASK_LOAD_DIM_STORE
    WAREHOUSE = TRANSFORM_WH
    AFTER TASK_ETL_ROOT
    COMMENT = 'Load store dimension'
AS
    MERGE INTO DIM_STORE target
    USING (
        SELECT * FROM STAGING.STG_STORES_STREAM
        WHERE METADATA$ACTION = 'INSERT' AND dq_is_valid = TRUE
    ) source
    ON target.store_id = source.store_id
    WHEN MATCHED THEN UPDATE SET
        target.store_name = source.store_name,
        target.store_type = source.store_type,
        target.city = source.city,
        target.state = source.state,
        target.region = source.region,
        target.is_active = source.is_active
    WHEN NOT MATCHED THEN INSERT (
        store_id, store_name, store_type, city, state, region,
        country, is_active, open_date
    ) VALUES (
        source.store_id, source.store_name, source.store_type,
        source.city, source.state, source.region, source.country,
        source.is_active, source.open_date
    );

-- =============================================================================
-- SECTION 3: FACT LOADING TASKS (Level 2 - depends on dimensions)
-- =============================================================================

-- Load Sales Fact
CREATE OR REPLACE TASK TASK_LOAD_FACT_SALES
    WAREHOUSE = TRANSFORM_WH
    AFTER TASK_LOAD_DIM_CUSTOMER, TASK_LOAD_DIM_PRODUCT, TASK_LOAD_DIM_STORE
    COMMENT = 'Load sales fact table after all dimensions are loaded'
AS
    CALL SP_LOAD_FACT_SALES();

-- Load Clickstream Fact
CREATE OR REPLACE TASK TASK_LOAD_FACT_CLICKSTREAM
    WAREHOUSE = TRANSFORM_WH
    AFTER TASK_LOAD_DIM_CUSTOMER, TASK_LOAD_DIM_PRODUCT
    COMMENT = 'Load clickstream fact table'
AS
    INSERT INTO FACT_CLICKSTREAM (
        event_id, date_key, customer_key, session_id,
        event_timestamp, event_type, page_url, referrer_url,
        device_type, browser, duration_seconds, product_id,
        order_id, order_value, source_file
    )
    SELECT
        s.event_id,
        d.date_key,
        c.customer_key,
        s.session_id,
        s.event_timestamp,
        s.event_type,
        s.page_url,
        s.referrer_url,
        s.device_type,
        s.browser,
        s.duration_seconds,
        s.product_id,
        s.order_id,
        s.order_value,
        s.source_file
    FROM STAGING.STG_CLICKSTREAM_STREAM s
    LEFT JOIN DIM_DATE d ON DATE(s.event_timestamp) = d.full_date
    LEFT JOIN DIM_CUSTOMER c ON s.customer_id = c.customer_id AND c.is_current = TRUE
    WHERE s.dq_is_valid = TRUE;

-- =============================================================================
-- SECTION 4: ANALYTICS REFRESH TASKS (Level 3)
-- =============================================================================

-- Refresh materialized views and aggregations
CREATE OR REPLACE TASK TASK_REFRESH_ANALYTICS
    WAREHOUSE = TRANSFORM_WH
    AFTER TASK_LOAD_FACT_SALES, TASK_LOAD_FACT_CLICKSTREAM
    COMMENT = 'Refresh analytics layer after fact loading'
AS
    -- Refresh aggregations
    CALL SP_REFRESH_AGGREGATIONS();

-- =============================================================================
-- SECTION 5: DATA QUALITY TASK (Level 4)
-- =============================================================================

CREATE OR REPLACE TASK TASK_DATA_QUALITY_CHECK
    WAREHOUSE = TRANSFORM_WH
    AFTER TASK_REFRESH_ANALYTICS
    COMMENT = 'Run data quality checks after all processing'
AS
    CALL SP_RUN_DATA_QUALITY_CHECKS();

-- =============================================================================
-- SECTION 6: TASK MANAGEMENT
-- =============================================================================

-- Resume all tasks (tasks are created in suspended state)
ALTER TASK TASK_DATA_QUALITY_CHECK RESUME;
ALTER TASK TASK_REFRESH_ANALYTICS RESUME;
ALTER TASK TASK_LOAD_FACT_CLICKSTREAM RESUME;
ALTER TASK TASK_LOAD_FACT_SALES RESUME;
ALTER TASK TASK_LOAD_DIM_STORE RESUME;
ALTER TASK TASK_LOAD_DIM_PRODUCT RESUME;
ALTER TASK TASK_LOAD_DIM_CUSTOMER RESUME;
ALTER TASK TASK_ETL_ROOT RESUME;  -- Resume root last!

-- Check task status
SHOW TASKS IN SCHEMA CURATED;

-- View task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
ORDER BY SCHEDULED_TIME DESC;

-- =============================================================================
-- SECTION 7: TASK DAG VISUALIZATION
-- =============================================================================
/*
Task Dependency Tree:

TASK_ETL_ROOT (scheduled hourly)
    │
    ├──▶ TASK_LOAD_DIM_CUSTOMER (SCD2)
    │
    ├──▶ TASK_LOAD_DIM_PRODUCT (SCD1)
    │
    └──▶ TASK_LOAD_DIM_STORE
              │
              └──────────────────┬───────────────────┐
                                 │                   │
                                 ▼                   ▼
                    TASK_LOAD_FACT_SALES    TASK_LOAD_FACT_CLICKSTREAM
                                 │                   │
                                 └─────────┬─────────┘
                                           │
                                           ▼
                               TASK_REFRESH_ANALYTICS
                                           │
                                           ▼
                               TASK_DATA_QUALITY_CHECK
*/

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How do you handle task failures?
A1: - Tasks retry automatically based on configuration
    - Check TASK_HISTORY for error messages
    - Use stored procedures with TRY/CATCH for graceful handling
    - Set up alerts on task failures

Q2: What's the difference between SCHEDULE and AFTER?
A2: - SCHEDULE: Cron-based or interval trigger
    - AFTER: Dependency-based (runs after predecessor completes)
    - Only root tasks have SCHEDULE; children use AFTER

Q3: How do you pause the entire pipeline?
A3: Suspend the root task: ALTER TASK TASK_ETL_ROOT SUSPEND;
    All child tasks won't run since root doesn't trigger them.
*/
