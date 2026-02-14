# Snowflake Troubleshooting Guide

Common issues, diagnostic approaches, and solutions for Snowflake data engineering.

---

## Table of Contents
1. [Data Loading Issues](#data-loading-issues)
2. [Query Performance Issues](#query-performance-issues)
3. [Pipeline & Task Issues](#pipeline--task-issues)
4. [Security & Access Issues](#security--access-issues)
5. [Cost & Resource Issues](#cost--resource-issues)
6. [Snowpipe Issues](#snowpipe-issues)
7. [Diagnostic Queries](#diagnostic-queries)

---

## Data Loading Issues

### COPY Command Skips Files
**Symptom:** COPY INTO completes but reports 0 rows loaded.

**Possible Causes & Solutions:**

| Cause | Solution |
|-------|----------|
| Files already loaded | Use `FORCE = TRUE` to reload, or check `COPY_HISTORY` |
| File format mismatch | Verify FILE_FORMAT matches actual data format |
| Empty files | Check source files have data |
| Wrong stage path | Use `LIST @stage` to verify files exist |

```sql
-- Check what files were loaded/skipped
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MY_TABLE',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
));

-- Force reload already-loaded files
COPY INTO my_table FROM @my_stage FORCE = TRUE;
```

### Date/Timestamp Parsing Errors
**Symptom:** `Date 'xxx' is not recognized`

**Solution:**
```sql
-- Specify date format explicitly
FILE_FORMAT = (
    TYPE = 'CSV'
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
);

-- Or use AUTO detection
FILE_FORMAT = (
    TYPE = 'CSV'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
);
```

### Column Count Mismatch
**Symptom:** `Number of columns in file does not match`

**Solution:**
```sql
-- Allow mismatch (loads available columns)
FILE_FORMAT = (ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE);

-- Or select specific columns using a query
COPY INTO my_table (col1, col2, col3)
FROM (SELECT $1, $2, $3 FROM @my_stage)
FILE_FORMAT = (TYPE = 'CSV');
```

### JSON Loading Failures
**Symptom:** JSON parsing errors during COPY

**Solution:**
```sql
-- Strip outer array if present
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

-- Handle embedded newlines
FILE_FORMAT = (TYPE = 'JSON' STRIP_NULL_VALUES = TRUE);

-- Load raw, then parse
COPY INTO raw_table (raw_json)
FROM (SELECT $1 FROM @my_stage)
FILE_FORMAT = (TYPE = 'JSON');
```

---

## Query Performance Issues

### Query Takes Too Long

**Step-by-step diagnosis:**

1. **Check Query Profile** (Snowflake UI → Query History → Profile)
2. **Look for these red flags:**

| Red Flag | Meaning | Solution |
|----------|---------|----------|
| Full table scan | No partition pruning | Add clustering key or filter on clustered columns |
| Spilling to disk | Insufficient memory | Use larger warehouse |
| Spilling to remote | Severe memory issue | Use much larger warehouse or optimize query |
| Cartesian product | Missing/bad JOIN condition | Fix JOIN clause |
| High queue time | Warehouse overloaded | Scale up or add clusters |

```sql
-- Check for spilling
SELECT QUERY_ID, WAREHOUSE_SIZE,
       BYTES_SPILLED_TO_LOCAL_STORAGE / 1e9 AS gb_spilled_local,
       BYTES_SPILLED_TO_REMOTE_STORAGE / 1e9 AS gb_spilled_remote
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_ID = 'your-query-id';

-- Check pruning effectiveness
SELECT PARTITIONS_SCANNED, PARTITIONS_TOTAL,
       ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 2) AS scan_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_ID = 'your-query-id';
```

### Result Cache Not Working
**Symptom:** Same query runs full compute every time.

**Reasons cache won't be used:**
- Different user roles
- Underlying data changed
- Query contains non-deterministic functions (`CURRENT_TIMESTAMP`, `RANDOM`)
- Query uses external functions
- More than 24 hours since last cache

```sql
-- Verify cache setting
SHOW PARAMETERS LIKE 'USE_CACHED_RESULT' IN SESSION;

-- Enable if disabled
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
```

---

## Pipeline & Task Issues

### Task Not Running
**Symptom:** Task is created but never executes.

**Checklist:**
```sql
-- 1. Check if task is RESUMED (not suspended)
SHOW TASKS LIKE 'MY_TASK';
-- Look for "state" = "started"

-- 2. Resume the task
ALTER TASK MY_TASK RESUME;

-- 3. If task has a predecessor, resume from root first
-- Resume root task LAST (after all children)
ALTER TASK CHILD_TASK RESUME;
ALTER TASK ROOT_TASK RESUME;

-- 4. Check task history for errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'MY_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- 5. Verify warehouse exists and is not suspended
SHOW WAREHOUSES LIKE 'TRANSFORM_WH';
```

### Stream Has No Data
**Symptom:** Stream shows 0 rows when you expect changes.

**Causes:**
- Stream was already consumed (offset advanced)
- No DML occurred on the source table since stream creation
- Stream went stale (past retention period)

```sql
-- Check stream status
SHOW STREAMS LIKE 'MY_STREAM';

-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('MY_STREAM');

-- Check stream staleness
SELECT
    NAME, STALE, STALE_AFTER
FROM TABLE(INFORMATION_SCHEMA.STREAMS())
WHERE NAME = 'MY_STREAM';

-- If stale, recreate the stream
CREATE OR REPLACE STREAM MY_STREAM ON TABLE MY_TABLE;
```

---

## Security & Access Issues

### "Access denied" or "Insufficient privileges"

**Diagnosis steps:**
```sql
-- 1. Check your current role
SELECT CURRENT_ROLE();

-- 2. Check grants on the object
SHOW GRANTS ON TABLE schema.table_name;

-- 3. Check grants to your role
SHOW GRANTS TO ROLE MY_ROLE;

-- 4. Check role hierarchy
SHOW GRANTS OF ROLE PARENT_ROLE;

-- 5. Fix: Grant the needed privilege
GRANT SELECT ON TABLE schema.table_name TO ROLE MY_ROLE;
GRANT USAGE ON SCHEMA schema_name TO ROLE MY_ROLE;
GRANT USAGE ON DATABASE database_name TO ROLE MY_ROLE;
-- Remember: Need USAGE on DB AND schema AND privilege on object
```

### Storage Integration Access Denied
**Symptom:** `Access denied` when querying external stage.

**Checklist:**
1. Verify IAM role trust policy has correct Snowflake ARN and External ID
2. Verify IAM permissions include the S3 bucket and path
3. Verify `STORAGE_ALLOWED_LOCATIONS` includes the path
4. Run `DESC INTEGRATION` to get the latest ARN values

```sql
DESC INTEGRATION S3_RETAIL_INTEGRATION;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Update AWS IAM trust policy with these values
```

---

## Cost & Resource Issues

### Unexpected High Costs

**Investigation queries:**
```sql
-- Top credit consumers (last 7 days)
SELECT WAREHOUSE_NAME, ROUND(SUM(CREDITS_USED), 2) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 2 DESC;

-- Find queries consuming most credits
SELECT USER_NAME, WAREHOUSE_NAME, QUERY_TEXT,
       EXECUTION_TIME / 1000 AS seconds,
       CREDITS_USED_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY EXECUTION_TIME DESC LIMIT 20;

-- Check serverless costs
SELECT SERVICE_TYPE, SUM(CREDITS_USED) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
WHERE START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1;
```

### Warehouse Won't Suspend
**Symptom:** Warehouse stays running, burning credits.

**Causes:**
- `AUTO_SUSPEND` set too high or to 0 (never)
- Active queries still running
- Tasks scheduled frequently keeping it awake

```sql
-- Check auto-suspend setting
SHOW WAREHOUSES LIKE 'MY_WH';

-- Set aggressive auto-suspend for dev
ALTER WAREHOUSE MY_WH SET AUTO_SUSPEND = 60;  -- 1 minute

-- Manually suspend
ALTER WAREHOUSE MY_WH SUSPEND;
```

---

## Snowpipe Issues

### Snowpipe Not Loading Files

**Diagnostic flow:**
```sql
-- 1. Check pipe status
SELECT SYSTEM$PIPE_STATUS('my_pipe');
-- Should show: "executionState": "RUNNING"

-- 2. Check recent copy history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MY_TABLE',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
)) ORDER BY LAST_LOAD_TIME DESC;

-- 3. Verify SNS subscription
DESC PIPE my_pipe;
-- Check notification_channel

-- 4. Force a manual refresh
ALTER PIPE my_pipe REFRESH;
```

### Duplicate Records from Snowpipe
**Symptom:** Same data loaded multiple times.

**Causes:**
- File was modified and re-uploaded with same name
- `FORCE = TRUE` was used
- Multiple pipes loading same files

**Prevention:**
- Use unique file names (include timestamp)
- Don't modify files after upload
- Implement deduplication in staging layer

---

## Diagnostic Queries

### General Health Check
```sql
-- Account status
SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_VERSION();

-- Active warehouses
SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));

-- Running queries
SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS, START_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE())
WHERE EXECUTION_STATUS = 'RUNNING';

-- Failed queries (last hour)
SELECT QUERY_ID, ERROR_CODE, ERROR_MESSAGE, QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('hour', -1, CURRENT_TIMESTAMP())
AND ERROR_CODE IS NOT NULL
ORDER BY START_TIME DESC;
```

### Pipeline Status Dashboard Query
```sql
-- Comprehensive pipeline health
SELECT
    'Tasks' AS component,
    COUNT(*) AS total,
    SUM(CASE WHEN STATE = 'SUCCEEDED' THEN 1 ELSE 0 END) AS healthy,
    SUM(CASE WHEN STATE = 'FAILED' THEN 1 ELSE 0 END) AS issues
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
));
```

---

## Quick Reference: Error Codes

| Error | Meaning | Quick Fix |
|-------|---------|-----------|
| 000606 | Table does not exist | Check spelling, schema, and database context |
| 001003 | Object does not exist | Verify role has USAGE on database/schema |
| 002003 | SQL compilation error | Check SQL syntax |
| 090106 | Warehouse does not exist | Create warehouse or check spelling |
| 100035 | Network policy denied | Add your IP to the network policy |
| 100038 | Login failed | Check credentials and MFA |
