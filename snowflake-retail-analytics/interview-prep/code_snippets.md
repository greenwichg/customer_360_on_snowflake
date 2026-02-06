# Reusable Snowflake SQL Patterns & Code Snippets

A collection of production-ready SQL patterns for common Snowflake tasks. Use these as building blocks for your projects and as reference during interviews.

---

## Table of Contents
1. [Data Loading Patterns](#data-loading-patterns)
2. [SCD Implementations](#scd-implementations)
3. [Window Functions](#window-functions)
4. [Semi-Structured Data](#semi-structured-data)
5. [Performance Optimization](#performance-optimization)
6. [Security Patterns](#security-patterns)
7. [Monitoring Queries](#monitoring-queries)
8. [Useful Utility Patterns](#useful-utility-patterns)

---

## Data Loading Patterns

### COPY INTO with Error Handling
```sql
COPY INTO staging.stg_sales
FROM @landing.sales_stage/daily/
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE'           -- Skip bad rows
SIZE_LIMIT = 5368709120         -- 5GB per batch
PURGE = FALSE                   -- Keep source files
FORCE = FALSE                   -- Skip already-loaded files
RETURN_FAILED_ONLY = TRUE;      -- Show only failures

-- Check load history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STG_SALES',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));
```

### Snowpipe with Auto-Ingest
```sql
CREATE OR REPLACE PIPE landing.sales_pipe
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:123456:notifications'
AS
COPY INTO staging.stg_sales
FROM @landing.sales_stage
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'SKIP_FILE';

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('landing.sales_pipe');
```

### Load JSON with Flattening
```sql
COPY INTO staging.stg_events (raw_data, source_file, load_ts)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()
    FROM @landing.events_stage
)
FILE_FORMAT = (TYPE = 'JSON');

-- Flatten nested JSON into relational format
INSERT INTO staging.stg_events_flat
SELECT
    raw_data:event_id::STRING AS event_id,
    raw_data:event_type::STRING AS event_type,
    raw_data:timestamp::TIMESTAMP AS event_ts,
    raw_data:properties:product_id::STRING AS product_id,
    f.value:key::STRING AS attribute_name,
    f.value:value::STRING AS attribute_value
FROM staging.stg_events,
    LATERAL FLATTEN(input => raw_data:properties) f;
```

---

## SCD Implementations

### SCD Type 2 with MERGE (Stream-Based)
```sql
-- Step 1: Expire changed records
UPDATE dim_customer SET
    end_date = DATEADD('day', -1, CURRENT_DATE()),
    is_current = FALSE,
    updated_timestamp = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
AND customer_id IN (
    SELECT customer_id FROM stg_customers_stream
    WHERE METADATA$ACTION = 'INSERT'
)
AND record_hash != (
    SELECT record_hash FROM stg_customers_stream s
    WHERE s.customer_id = dim_customer.customer_id
);

-- Step 2: Insert new versions
INSERT INTO dim_customer (customer_id, ..., effective_date, is_current)
SELECT customer_id, ..., CURRENT_DATE(), TRUE
FROM stg_customers_stream
WHERE METADATA$ACTION = 'INSERT'
AND NOT EXISTS (
    SELECT 1 FROM dim_customer d
    WHERE d.customer_id = stg_customers_stream.customer_id
    AND d.is_current = TRUE
    AND d.record_hash = stg_customers_stream.record_hash
);
```

### SCD Type 1 with MERGE
```sql
MERGE INTO dim_product target
USING staging.stg_products source
ON target.product_id = source.product_id
WHEN MATCHED AND target.record_hash != source.record_hash THEN
    UPDATE SET
        product_name = source.product_name,
        category = source.category,
        unit_price = source.unit_price,
        updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, unit_price)
    VALUES (source.product_id, source.product_name, source.category, source.unit_price);
```

### Record Hash for Change Detection
```sql
-- Generate a hash of all trackable columns for change detection
SELECT
    customer_id,
    MD5(
        COALESCE(first_name, '') || '|' ||
        COALESCE(last_name, '') || '|' ||
        COALESCE(email, '') || '|' ||
        COALESCE(city, '') || '|' ||
        COALESCE(state, '')
    ) AS record_hash
FROM staging.stg_customers;
```

---

## Window Functions

### Running Totals and Moving Averages
```sql
SELECT
    sale_date,
    daily_revenue,
    -- Running total (YTD)
    SUM(daily_revenue) OVER (
        PARTITION BY YEAR(sale_date)
        ORDER BY sale_date
    ) AS ytd_revenue,
    -- 7-day moving average
    AVG(daily_revenue) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d,
    -- 30-day moving average
    AVG(daily_revenue) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30d
FROM daily_sales;
```

### Ranking and Top-N
```sql
-- Top 3 products per category by revenue
SELECT * FROM (
    SELECT
        category,
        product_name,
        total_revenue,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rank,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS dense_rank
    FROM product_sales
)
WHERE rank <= 3;
```

### Lead/Lag for Period Comparisons
```sql
SELECT
    month,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY month) AS prev_month,
    LAG(revenue, 12) OVER (ORDER BY month) AS same_month_ly,
    ROUND((revenue - LAG(revenue, 1) OVER (ORDER BY month))
          / NULLIF(LAG(revenue, 1) OVER (ORDER BY month), 0) * 100, 2) AS mom_pct,
    ROUND((revenue - LAG(revenue, 12) OVER (ORDER BY month))
          / NULLIF(LAG(revenue, 12) OVER (ORDER BY month), 0) * 100, 2) AS yoy_pct
FROM monthly_sales;
```

### Sessionization (Clickstream)
```sql
-- Assign session IDs based on 30-minute inactivity gap
SELECT
    customer_id,
    event_timestamp,
    event_type,
    CONDITIONAL_CHANGE_EVENT(
        CASE WHEN DATEDIFF('minute', LAG(event_timestamp) OVER
            (PARTITION BY customer_id ORDER BY event_timestamp), event_timestamp) > 30
        THEN 1 ELSE 0 END
    ) OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS session_number
FROM clickstream_events;
```

---

## Semi-Structured Data

### Query VARIANT Columns
```sql
-- Dot notation
SELECT raw:event_type::STRING FROM events;

-- Bracket notation (for special characters)
SELECT raw['event-type']::STRING FROM events;

-- Nested access
SELECT raw:geo:country::STRING, raw:properties:price::NUMBER(10,2) FROM events;

-- Check if key exists
SELECT * FROM events WHERE raw:properties:coupon_code IS NOT NULL;
```

### FLATTEN for Arrays
```sql
-- Flatten array of items in an order
SELECT
    order_id,
    f.value:product_id::STRING AS product_id,
    f.value:quantity::INTEGER AS quantity,
    f.value:price::DECIMAL(10,2) AS price
FROM orders,
    LATERAL FLATTEN(input => order_data:items) f;
```

### OBJECT_CONSTRUCT for Building JSON
```sql
SELECT OBJECT_CONSTRUCT(
    'customer_id', customer_id,
    'name', full_name,
    'metrics', OBJECT_CONSTRUCT(
        'total_orders', total_orders,
        'lifetime_value', lifetime_value
    )
) AS customer_json
FROM dim_customer
WHERE is_current = TRUE;
```

---

## Performance Optimization

### Pruning Check
```sql
-- Check if your filter leverages partition pruning
SELECT
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 2) AS scan_pct
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
WHERE QUERY_ID = LAST_QUERY_ID();
```

### Clustering Depth Check
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('schema.table_name', '(column1, column2)');
```

### Efficient Date Filtering
```sql
-- Use integer date_key for faster pruning (vs DATE comparison)
WHERE date_key BETWEEN 20240101 AND 20240131  -- Fast (integer comparison)
-- vs
WHERE full_date BETWEEN '2024-01-01' AND '2024-01-31'  -- Slower
```

---

## Security Patterns

### Dynamic Masking
```sql
CREATE MASKING POLICY mask_pii AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ADMIN') THEN val
        ELSE SHA2(val)
    END;

ALTER TABLE customers MODIFY COLUMN ssn SET MASKING POLICY mask_pii;
```

### Row Access Policy
```sql
CREATE ROW ACCESS POLICY region_filter AS (region_col VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ADMIN'
    OR region_col IN (
        SELECT region FROM user_access WHERE user_name = CURRENT_USER()
    );

ALTER TABLE sales ADD ROW ACCESS POLICY region_filter ON (region);
```

---

## Monitoring Queries

### Find Expensive Queries
```sql
SELECT QUERY_ID, QUERY_TEXT, EXECUTION_TIME/1000 AS sec,
       BYTES_SCANNED/1e9 AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY EXECUTION_TIME DESC LIMIT 10;
```

### Credit Usage This Month
```sql
SELECT SUM(CREDITS_USED) AS total_credits,
       ROUND(SUM(CREDITS_USED) * 3, 2) AS estimated_cost
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE DATE_TRUNC('month', START_TIME) = DATE_TRUNC('month', CURRENT_DATE());
```

---

## Useful Utility Patterns

### Generate Date Sequences
```sql
SELECT DATEADD('day', SEQ4(), '2020-01-01')::DATE AS date_value
FROM TABLE(GENERATOR(ROWCOUNT => 2192));  -- ~6 years
```

### Pivot Table
```sql
SELECT * FROM monthly_sales
PIVOT (SUM(revenue) FOR month_name IN ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'));
```

### Unpivot Table
```sql
SELECT product_id, attribute_name, attribute_value
FROM product_attributes
UNPIVOT (attribute_value FOR attribute_name IN (color, size, weight, material));
```

### Conditional Aggregation
```sql
SELECT
    region,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled,
    ROUND(SUM(CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS completion_rate
FROM fact_sales
GROUP BY region;
```
