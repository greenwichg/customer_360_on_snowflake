/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - RESOURCE MONITORS SETUP
================================================================================
Purpose: Create resource monitors for cost control and alerting
Concepts: Credit quotas, notification triggers, warehouse assignment

Interview Points:
- Resource monitors track credit consumption
- Can trigger notifications or suspend warehouses at thresholds
- Essential for cost governance in production environments
================================================================================
*/

USE ROLE ACCOUNTADMIN;  -- Required for resource monitor creation

-- =============================================================================
-- SECTION 1: ACCOUNT-LEVEL RESOURCE MONITOR
-- =============================================================================
/*
Purpose: Overall account spending limit for the month
Action: Notify at 75%, 90%, 100%, then suspend at 110%
*/

CREATE OR REPLACE RESOURCE MONITOR ACCOUNT_MONTHLY_MONITOR
WITH
    CREDIT_QUOTA = 400                    -- Monthly credit limit (adjust based on budget)
    FREQUENCY = MONTHLY                    -- Reset period
    START_TIMESTAMP = IMMEDIATELY          -- Start monitoring now
    END_TIMESTAMP = NULL                   -- No end date (continuous)
    TRIGGERS
        ON 50 PERCENT DO NOTIFY            -- Early warning
        ON 75 PERCENT DO NOTIFY            -- Alert: 3/4 budget used
        ON 90 PERCENT DO NOTIFY            -- Critical: approaching limit
        ON 100 PERCENT DO NOTIFY           -- At limit
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;  -- Hard stop (with buffer)

-- Note: SUSPEND vs SUSPEND_IMMEDIATE
-- SUSPEND: Allows running queries to complete
-- SUSPEND_IMMEDIATE: Kills running queries immediately

-- =============================================================================
-- SECTION 2: WAREHOUSE-SPECIFIC RESOURCE MONITORS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 LOADING WAREHOUSE MONITOR (Daily)
-- -----------------------------------------------------------------------------
/*
Purpose: Limit daily spending on data loading
Rationale: Loading should be predictable; unusual spikes indicate issues
*/

CREATE OR REPLACE RESOURCE MONITOR LOADING_WH_DAILY
WITH
    CREDIT_QUOTA = 10                      -- 10 credits/day for loading
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Assign to Loading Warehouse
ALTER WAREHOUSE LOADING_WH SET RESOURCE_MONITOR = LOADING_WH_DAILY;

-- -----------------------------------------------------------------------------
-- 2.2 TRANSFORM WAREHOUSE MONITOR (Daily)
-- -----------------------------------------------------------------------------
/*
Purpose: Limit ETL/transformation spending
Rationale: Tasks run on schedule; spikes may indicate runaway processes
*/

CREATE OR REPLACE RESOURCE MONITOR TRANSFORM_WH_DAILY
WITH
    CREDIT_QUOTA = 20                      -- 20 credits/day for ETL
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE TRANSFORM_WH SET RESOURCE_MONITOR = TRANSFORM_WH_DAILY;

-- -----------------------------------------------------------------------------
-- 2.3 ANALYTICS WAREHOUSE MONITOR (Daily)
-- -----------------------------------------------------------------------------
/*
Purpose: Limit analytics/BI query spending
Rationale: Largest warehouse; needs careful monitoring
Note: Higher limit due to multi-cluster and larger size
*/

CREATE OR REPLACE RESOURCE MONITOR ANALYTICS_WH_DAILY
WITH
    CREDIT_QUOTA = 50                      -- 50 credits/day for analytics
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = ANALYTICS_WH_DAILY;

-- -----------------------------------------------------------------------------
-- 2.4 DEVELOPMENT WAREHOUSE MONITOR (Weekly)
-- -----------------------------------------------------------------------------
/*
Purpose: Loose limit for development work
Rationale: Developers need flexibility but shouldn't have unlimited spend
*/

CREATE OR REPLACE RESOURCE MONITOR DEV_WH_WEEKLY
WITH
    CREDIT_QUOTA = 25                      -- 25 credits/week for dev
    FREQUENCY = WEEKLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE DEV_WH SET RESOURCE_MONITOR = DEV_WH_WEEKLY;

-- =============================================================================
-- SECTION 3: VERIFY RESOURCE MONITOR SETUP
-- =============================================================================

-- Show all resource monitors
SHOW RESOURCE MONITORS;

-- Detailed view
SELECT
    "name" AS monitor_name,
    "credit_quota" AS credit_limit,
    "used_credits" AS credits_used,
    "remaining_credits" AS credits_remaining,
    "frequency" AS reset_frequency,
    "start_time",
    "end_time",
    "notify_at" AS notify_thresholds,
    "suspend_at" AS suspend_threshold,
    "suspend_immediately_at" AS immediate_suspend
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Check warehouse assignments
SHOW WAREHOUSES;
SELECT
    "name" AS warehouse_name,
    "resource_monitor" AS assigned_monitor,
    "size",
    "state"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- =============================================================================
-- SECTION 4: CREDIT USAGE MONITORING QUERIES
-- =============================================================================

-- Current credit usage by warehouse (last 24 hours)
/*
SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME
ORDER BY total_credits DESC;
*/

-- Daily credit trend (last 7 days)
/*
SELECT
    DATE_TRUNC('day', START_TIME) AS usage_date,
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS daily_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
*/

-- Projected monthly spend
/*
WITH daily_usage AS (
    SELECT
        DATE_TRUNC('day', START_TIME) AS usage_date,
        SUM(CREDITS_USED) AS daily_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY 1
)
SELECT
    AVG(daily_credits) AS avg_daily_credits,
    AVG(daily_credits) * 30 AS projected_monthly_credits,
    AVG(daily_credits) * 30 * 3 AS projected_monthly_cost_usd  -- ~$3/credit estimate
FROM daily_usage;
*/

-- =============================================================================
-- SECTION 5: ALERT NOTIFICATION SETUP
-- =============================================================================
/*
Resource monitor notifications go to account administrators by default.
For custom notifications, you can:
1. Set up email notifications in Account → Notifications
2. Use Snowflake Alerts + external integrations
3. Implement custom alerting via tasks + external_access

Example custom alert (requires external access integration):
CREATE OR REPLACE ALERT credit_usage_alert
  WAREHOUSE = DEV_WH
  SCHEDULE = 'USING CRON 0 9 * * * UTC'
  IF (EXISTS (
    SELECT 1 FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
      DATE_RANGE_START => DATEADD('day', -1, CURRENT_DATE()),
      DATE_RANGE_END => CURRENT_DATE()
    ))
    HAVING SUM(CREDITS_USED) > 20
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'alerts@company.com',
      'Snowflake Credit Alert',
      'Daily credit usage exceeded threshold'
    );
*/

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: What actions can resource monitors take?
A1: Three actions:
    - NOTIFY: Send notification (email to admins)
    - SUSPEND: Prevent new queries, let running queries complete
    - SUSPEND_IMMEDIATE: Kill running queries immediately

Q2: Can you have multiple resource monitors on one warehouse?
A2: No, each warehouse can only have ONE resource monitor assigned.
    However, one resource monitor can cover multiple warehouses.

Q3: What happens when a warehouse is suspended by resource monitor?
A3:
    - New queries return error: "Warehouse suspended"
    - For SUSPEND: Running queries continue to completion
    - For SUSPEND_IMMEDIATE: Running queries are killed
    - Warehouse remains suspended until:
      a) Monitor resets (next period)
      b) Credit quota is increased
      c) Monitor is removed/changed

Q4: How do you handle unexpected credit spikes?
A4:
    1. Set up resource monitors with conservative limits
    2. Use multiple notification thresholds (50%, 75%, 90%)
    3. Review WAREHOUSE_METERING_HISTORY regularly
    4. Identify expensive queries via QUERY_HISTORY
    5. Optimize or cancel runaway queries

Q5: What costs are NOT covered by resource monitors?
A5: Resource monitors track WAREHOUSE credits only. They don't cover:
    - Storage costs
    - Serverless features (Snowpipe, serverless tasks)
    - Cloud services layer (if >10% of compute)
    - Data transfer/egress

Q6: How do you set appropriate credit quotas?
A6:
    1. Baseline current usage (run for 1-2 weeks without limits)
    2. Add buffer for growth (20-30%)
    3. Consider business cycles (month-end reports, etc.)
    4. Set conservative initial limits, adjust based on actual usage
    5. Different quotas for different warehouses based on workload
*/

-- =============================================================================
-- SECTION 6: RESOURCE MONITOR MANAGEMENT
-- =============================================================================

-- Modify quota (e.g., increase for end-of-month processing)
-- ALTER RESOURCE MONITOR ANALYTICS_WH_DAILY SET CREDIT_QUOTA = 75;

-- Remove resource monitor from warehouse
-- ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = NULL;

-- Drop resource monitor
-- DROP RESOURCE MONITOR IF EXISTS DEV_WH_WEEKLY;

-- Reset used credits (by recreating with same settings)
-- This effectively resets the counter

-- Temporarily disable monitoring (not recommended for production)
-- ALTER RESOURCE MONITOR ACCOUNT_MONTHLY_MONITOR SUSPEND;
-- ALTER RESOURCE MONITOR ACCOUNT_MONTHLY_MONITOR RESUME;
