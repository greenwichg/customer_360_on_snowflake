# Snowflake Interview Questions & Answers

## Table of Contents
1. [Architecture](#architecture)
2. [Data Loading](#data-loading)
3. [Performance Optimization](#performance-optimization)
4. [Security](#security)
5. [Cost Management](#cost-management)
6. [Scenario-Based Questions](#scenario-based-questions)

---

## Architecture

### Q1: Explain Snowflake's three-layer architecture
**Answer:**
```
┌─────────────────────────────────────────────────────┐
│           CLOUD SERVICES LAYER                       │
│  • Authentication & Authorization                    │
│  • Metadata Management                               │
│  • Query Optimization & Compilation                  │
│  • Infrastructure Management                         │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│           COMPUTE LAYER (Virtual Warehouses)         │
│  • Independent compute clusters                      │
│  • Elastically scalable                              │
│  • Pay per second (60-sec minimum)                   │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│           STORAGE LAYER                              │
│  • Centralized, shared storage                       │
│  • Columnar format, compressed                       │
│  • Automatic micro-partitioning                      │
└─────────────────────────────────────────────────────┘
```

**Key Points:**
- Separation enables independent scaling
- Storage is shared; compute is isolated
- Cloud services is "always on" (charges if >10% of compute)

---

### Q2: What are micro-partitions?
**Answer:**
- Snowflake's internal storage unit
- 50-500 MB compressed, ~16 million rows
- Columnar format with metadata (min/max, distinct count)
- Immutable (changes create new partitions)
- Enable pruning without explicit indexes

---

### Q3: Difference between Permanent, Transient, and Temporary tables?

| Feature | Permanent | Transient | Temporary |
|---------|-----------|-----------|-----------|
| Time Travel | 0-90 days | 0-1 day | 0-1 day |
| Fail-safe | 7 days | None | None |
| Storage Cost | Higher | Lower | Lowest |
| Visibility | All sessions | All sessions | Current session only |
| Use Case | Production data | Staging/intermediate | Session-specific temp data |

---

## Data Loading

### Q4: COPY INTO vs Snowpipe - when to use each?

**COPY INTO:**
- Batch loading from stages
- Uses virtual warehouse (charged by credit-hour)
- Manual or scheduled execution
- Best for: Large files, infrequent loads, complex transformations

**Snowpipe:**
- Serverless, event-driven
- Charged per file (~0.06 credits/1000 files)
- Near real-time (1-2 minute latency)
- Best for: Continuous small files, streaming data

---

### Q5: What are Streams and how do they work?

**Answer:**
Streams track DML changes (INSERT, UPDATE, DELETE) on tables:
```sql
CREATE STREAM sales_stream ON TABLE sales;
-- Stream captures changes until consumed
SELECT * FROM sales_stream WHERE METADATA$ACTION = 'INSERT';
-- After DML consuming stream, offset advances
```

**Metadata Columns:**
- `METADATA$ACTION`: 'INSERT' or 'DELETE'
- `METADATA$ISUPDATE`: TRUE if part of an UPDATE
- `METADATA$ROW_ID`: Unique change identifier

**Key Concepts:**
- Stream is "consumed" when used in DML
- Exactly-once processing guarantee
- Can become "stale" if not consumed within retention period

---

### Q6: How do you implement SCD Type 2?

**Answer:**
```sql
-- Step 1: Expire current records that changed
UPDATE dim_customer
SET end_date = CURRENT_DATE() - 1, is_current = FALSE
WHERE customer_id IN (
    SELECT customer_id FROM customer_stream
    WHERE METADATA$ISUPDATE = TRUE
) AND is_current = TRUE;

-- Step 2: Insert new versions
INSERT INTO dim_customer (customer_id, ..., effective_date, is_current)
SELECT customer_id, ..., CURRENT_DATE(), TRUE
FROM customer_stream
WHERE METADATA$ACTION = 'INSERT';
```

---

## Performance Optimization

### Q7: How do you optimize a slow query?

**Answer (Step by step):**

1. **Check Query Profile:**
   - Look for expensive operations
   - Identify data spilling
   - Check partition pruning

2. **Add Clustering Keys:**
   ```sql
   ALTER TABLE fact_sales CLUSTER BY (date_key, store_key);
   ```
   Use for columns frequently in WHERE/JOIN

3. **Right-size Warehouse:**
   - Spilling to remote = need larger warehouse
   - Queuing = need more clusters

4. **Use Materialized Views:**
   ```sql
   CREATE MATERIALIZED VIEW mv_daily_sales AS
   SELECT date, SUM(amount) FROM sales GROUP BY date;
   ```

5. **Enable Search Optimization:**
   ```sql
   ALTER TABLE customers ADD SEARCH OPTIMIZATION ON EQUALITY(email);
   ```

---

### Q8: Explain Snowflake's caching layers

**Answer:**
```
Query → Result Cache (24hr, free) → Warehouse Cache (SSD) → Remote Storage
```

1. **Result Cache (Cloud Services):**
   - Exact same query, same role = instant return
   - 24-hour retention
   - FREE - no compute used

2. **Warehouse Local Disk Cache:**
   - SSD on warehouse nodes
   - Faster than remote storage
   - Lost when warehouse suspends

3. **Remote Storage:**
   - Always available
   - Slowest option

---

### Q9: When should you use clustering keys?

**Answer:**
Use clustering when:
- Table is large (multi-TB)
- Queries frequently filter on specific columns
- Current clustering depth is poor

```sql
-- Check clustering quality
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_sales', '(date_key)');

-- Add clustering
ALTER TABLE fact_sales CLUSTER BY (date_key, region);
```

**Best Practices:**
- 3-4 columns maximum
- Low-cardinality first, then high
- Most selective columns in WHERE clauses

---

## Security

### Q10: How do you implement row-level security?

**Answer:**
```sql
-- Create mapping table
CREATE TABLE user_region_access (user_name VARCHAR, region VARCHAR);

-- Create row access policy
CREATE ROW ACCESS POLICY region_policy AS (region VARCHAR)
RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ADMIN'
    OR EXISTS (
        SELECT 1 FROM user_region_access
        WHERE user_name = CURRENT_USER()
        AND region = region
    );

-- Apply to table
ALTER TABLE sales ADD ROW ACCESS POLICY region_policy ON (region);
```

---

### Q11: Masking Policy vs Secure View?

**Masking Policy:**
- Applies to column directly
- Dynamic, role-based
- Transparent to users
- One policy per column

**Secure View:**
- Separate object
- Logic hidden from users
- Must query view instead of table
- More flexible logic

**When to use:**
- Masking: Simple role-based obfuscation
- Secure View: Complex filtering logic, multi-table joins

---

## Cost Management

### Q12: How do you control Snowflake costs?

**Answer:**

1. **Resource Monitors:**
   ```sql
   CREATE RESOURCE MONITOR daily_limit
   WITH CREDIT_QUOTA = 100
   TRIGGERS ON 75% DO NOTIFY
            ON 100% DO SUSPEND;
   ```

2. **Warehouse Sizing:**
   - Start small, scale as needed
   - Use AUTO_SUSPEND (60 seconds for dev)
   - Separate warehouses by workload

3. **Storage Optimization:**
   - Transient tables for staging
   - Shorter Time Travel retention
   - Regular cleanup of unused data

4. **Query Optimization:**
   - Avoid SELECT *
   - Use clustering for large tables
   - Leverage caching

---

### Q13: Snowflake cost components?

| Component | Billing |
|-----------|---------|
| Compute | Credits per second (60-sec min) |
| Storage | $ per TB per month |
| Data Transfer | $ per TB (egress only) |
| Serverless | Credits per file/compute |
| Cloud Services | Free if <10% of compute |

---

## Scenario-Based Questions

### Q14: Design a data pipeline for real-time clickstream data

**Answer:**
```
Kinesis → S3 → Snowpipe → RAW_TABLE → Stream → Task → FACT_TABLE
                              ↓
                    Materialized View
                              ↓
                        Dashboard
```

**Implementation:**
1. Kinesis Firehose writes to S3 (1-min batches)
2. S3 event triggers Snowpipe via SQS
3. Snowpipe loads to raw table
4. Stream captures new records
5. Task (every 5 min) processes to fact table
6. Materialized view for dashboard queries

---

### Q15: How would you migrate from on-prem Oracle to Snowflake?

**Answer:**

**Phase 1: Assessment**
- Inventory tables, views, procedures
- Identify Oracle-specific features
- Size the data

**Phase 2: Schema Conversion**
- Convert data types (CLOB → VARCHAR)
- Rewrite procedures (PL/SQL → Snowflake SQL)
- Design dimensional model

**Phase 3: Data Migration**
- Export to Parquet (preserves types)
- Stage in S3
- COPY INTO Snowflake

**Phase 4: Validation**
- Row counts
- Checksum verification
- Query result comparison

**Phase 5: Cutover**
- Parallel run period
- Point applications to Snowflake
- Decommission Oracle

---

### Q16: Your daily ETL is taking too long. How do you troubleshoot?

**Answer:**

1. **Identify bottleneck:**
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
   WHERE NAME LIKE 'ETL%' ORDER BY SCHEDULED_TIME DESC;
   ```

2. **Check query profile for slowest task**

3. **Common solutions:**
   - Increase warehouse size
   - Add clustering keys
   - Parallelize independent tasks
   - Use streams for incremental (not full) loads
   - Optimize MERGE statements

---

## Quick Reference - Key Differences

| Feature A | Feature B | Key Difference |
|-----------|-----------|----------------|
| VIEW | MATERIALIZED VIEW | MV stores data, auto-refreshes |
| COPY | INSERT | COPY tracks loaded files |
| STREAM | TIME TRAVEL | Stream for forward processing, TT for historical |
| CLONE | BACKUP | Clone is instant, zero storage |
| MASKING | ENCRYPTION | Masking is display-time, encryption at-rest |
| TASK | STORED PROCEDURE | Task is scheduled, SP is callable |

---

## Project-Specific Talking Points

"In my project, I..."

1. **Implemented SCD Type 2** using streams and MERGE for customer dimension
2. **Set up Snowpipe** for near-real-time clickstream ingestion
3. **Created a task DAG** with parent-child dependencies for ETL orchestration
4. **Applied masking policies** on PII columns based on user roles
5. **Used zero-copy cloning** to create development environments instantly
6. **Monitored costs** with resource monitors and query analysis
7. **Optimized queries** by adding clustering keys to fact tables
