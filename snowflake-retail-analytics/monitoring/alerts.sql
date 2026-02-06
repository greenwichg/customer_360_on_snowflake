/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - ALERTS & NOTIFICATIONS
================================================================================
Purpose: Set up automated alerts for pipeline monitoring
Concepts: Snowflake alerts, email notifications, task monitoring

Interview Points:
- Snowflake alerts evaluate conditions on a schedule
- Trigger notifications when conditions are met
- Can call stored procedures for automated remediation
- Complement resource monitors for comprehensive monitoring
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;
USE WAREHOUSE ANALYTICS_WH;

-- =============================================================================
-- SECTION 1: EMAIL NOTIFICATION INTEGRATION
-- =============================================================================

-- Create email notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_NOTIFICATION_INT
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = (
        'data-team@company.com',
        'oncall@company.com'
    )
    COMMENT = 'Email notifications for pipeline alerts';

-- =============================================================================
-- SECTION 2: PIPELINE FAILURE ALERT
-- =============================================================================

-- Alert when a task fails
CREATE OR REPLACE ALERT ALERT_TASK_FAILURE
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON */15 * * * * America/New_York'  -- Every 15 minutes
    IF (EXISTS (
        SELECT 1
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD('minute', -15, CURRENT_TIMESTAMP())
        ))
        WHERE STATE = 'FAILED'
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_NOTIFICATION_INT',
            'data-team@company.com',
            'ALERT: Snowflake Task Failure',
            'One or more tasks failed in the last 15 minutes. Check TASK_HISTORY for details.'
        );

-- ALTER ALERT ALERT_TASK_FAILURE RESUME;

-- =============================================================================
-- SECTION 3: DATA FRESHNESS ALERT
-- =============================================================================

-- Alert when data hasn't been loaded for 6+ hours
CREATE OR REPLACE ALERT ALERT_DATA_FRESHNESS
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 */2 * * * America/New_York'  -- Every 2 hours
    IF (EXISTS (
        SELECT 1
        FROM CURATED.FACT_SALES
        HAVING MAX(load_timestamp) < DATEADD('hour', -6, CURRENT_TIMESTAMP())
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_NOTIFICATION_INT',
            'data-team@company.com',
            'ALERT: Data Freshness - FACT_SALES Stale',
            'FACT_SALES has not received new data in over 6 hours. Last load: ' ||
            (SELECT MAX(load_timestamp)::VARCHAR FROM CURATED.FACT_SALES)
        );

-- ALTER ALERT ALERT_DATA_FRESHNESS RESUME;

-- =============================================================================
-- SECTION 4: DATA QUALITY ALERT
-- =============================================================================

-- Alert when data quality checks fail
CREATE OR REPLACE ALERT ALERT_DQ_FAILURE
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 7 * * * America/New_York'  -- 7 AM daily
    IF (EXISTS (
        SELECT 1
        FROM AUDIT.DQ_VALIDATION_LOG
        WHERE validation_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        AND status = 'FAIL'
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_NOTIFICATION_INT',
            'data-team@company.com',
            'ALERT: Data Quality Check Failures',
            'Data quality failures detected. Check AUDIT.VW_DQ_LATEST_RESULTS for details.'
        );

-- ALTER ALERT ALERT_DQ_FAILURE RESUME;

-- =============================================================================
-- SECTION 5: WAREHOUSE QUEUE ALERT
-- =============================================================================

-- Alert when queries are queuing (warehouse undersized)
CREATE OR REPLACE ALERT ALERT_WAREHOUSE_QUEUING
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON */30 * * * * America/New_York'  -- Every 30 minutes
    IF (EXISTS (
        SELECT 1
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME > DATEADD('minute', -30, CURRENT_TIMESTAMP())
        AND QUEUED_OVERLOAD_TIME > 30000  -- Queued > 30 seconds
        GROUP BY WAREHOUSE_NAME
        HAVING COUNT(*) > 5  -- More than 5 queued queries
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_NOTIFICATION_INT',
            'oncall@company.com',
            'ALERT: Warehouse Queuing Detected',
            'Multiple queries are queuing. Consider scaling up the warehouse or enabling multi-cluster.'
        );

-- ALTER ALERT ALERT_WAREHOUSE_QUEUING RESUME;

-- =============================================================================
-- SECTION 6: CREDIT USAGE ALERT
-- =============================================================================

-- Alert when daily credit usage exceeds threshold
CREATE OR REPLACE ALERT ALERT_HIGH_CREDIT_USAGE
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 20 * * * America/New_York'  -- 8 PM daily
    IF (EXISTS (
        SELECT 1
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE DATE(START_TIME) = CURRENT_DATE()
        HAVING SUM(CREDITS_USED) > 50  -- Threshold: 50 credits/day
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_NOTIFICATION_INT',
            'data-team@company.com',
            'ALERT: High Credit Usage Today',
            'Daily credit usage has exceeded 50 credits. Review warehouse activity.'
        );

-- ALTER ALERT ALERT_HIGH_CREDIT_USAGE RESUME;

-- =============================================================================
-- SECTION 7: SNOWPIPE FAILURE ALERT
-- =============================================================================

-- Alert when Snowpipe stops loading files
CREATE OR REPLACE ALERT ALERT_SNOWPIPE_STALE
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 */4 * * * America/New_York'  -- Every 4 hours
    IF (EXISTS (
        SELECT 1
        FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'LANDING.RAW_CLICKSTREAM',
            START_TIME => DATEADD('hour', -4, CURRENT_TIMESTAMP())
        ))
        HAVING COUNT(*) = 0  -- No files loaded in 4 hours
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_NOTIFICATION_INT',
            'data-team@company.com',
            'ALERT: Snowpipe - No Files Loaded (4 hours)',
            'Clickstream Snowpipe has not loaded any files in 4 hours. Check S3 events and pipe status.'
        );

-- ALTER ALERT ALERT_SNOWPIPE_STALE RESUME;

-- =============================================================================
-- SECTION 8: MANAGE ALERTS
-- =============================================================================

-- Show all alerts
SHOW ALERTS;

-- Check alert history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- Suspend all alerts (maintenance window)
-- ALTER ALERT ALERT_TASK_FAILURE SUSPEND;
-- ALTER ALERT ALERT_DATA_FRESHNESS SUSPEND;
-- ALTER ALERT ALERT_DQ_FAILURE SUSPEND;
-- ALTER ALERT ALERT_WAREHOUSE_QUEUING SUSPEND;
-- ALTER ALERT ALERT_HIGH_CREDIT_USAGE SUSPEND;
-- ALTER ALERT ALERT_SNOWPIPE_STALE SUSPEND;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How do Snowflake alerts work?
A1: Alerts evaluate a SQL condition on a schedule:
    - If the condition returns rows, the action is triggered
    - Actions can send emails or call stored procedures
    - Serverless execution (no dedicated warehouse needed for evaluation)
    - Schedule uses CRON syntax for flexibility

Q2: Alerts vs Resource Monitors?
A2: - Resource Monitors: Credit-based thresholds, suspend warehouses
    - Alerts: Custom SQL conditions, flexible actions
    - Use both together for comprehensive monitoring
    - Resource Monitors are more immediate (credit limits)
    - Alerts are more flexible (any SQL condition)

Q3: What should you monitor in a Snowflake data pipeline?
A3: Critical alerts:
    1. Task failures (pipeline breaks)
    2. Data freshness (SLA violations)
    3. Data quality (bad data propagation)
    4. Credit usage (cost control)
    5. Warehouse queuing (performance issues)
    6. Snowpipe failures (ingestion issues)
    7. Storage growth (capacity planning)
*/
