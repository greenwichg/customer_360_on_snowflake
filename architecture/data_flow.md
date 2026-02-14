# Data Flow Documentation

## End-to-End Data Pipeline

This document describes the complete data flow from source systems to analytics consumption.

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE OVERVIEW                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   SOURCE SYSTEMS              SNOWFLAKE LAYERS                    CONSUMERS     │
│                                                                                  │
│   ┌─────────────┐    ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│   │  AWS S3     │───▶│ LANDING │▶│ STAGING │▶│ CURATED │▶│ANALYTICS│──▶ BI     │
│   │  (Batch)    │    │  (RAW)  │ │  (ODS)  │ │  (DWH)  │ │ (MARTS) │           │
│   └─────────────┘    └─────────┘ └─────────┘ └─────────┘ └─────────┘           │
│                           ▲           │           │           │      ──▶ ML    │
│   ┌─────────────┐         │           │           │           │                 │
│   │  Kinesis    │─────────┘           │           │           │      ──▶ Apps  │
│   │ (Streaming) │                     │           │           │                 │
│   └─────────────┘                     ▼           ▼           ▼                 │
│                                  ┌─────────────────────────────┐                │
│   ┌─────────────┐                │     METADATA / GOVERNANCE   │                │
│   │  RDS MySQL  │───CDC──────────│  • Streams (CDC tracking)   │                │
│   │  (OLTP)     │                │  • Tasks (orchestration)    │                │
│   └─────────────┘                │  • Quality checks           │                │
│                                  └─────────────────────────────┘                │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Layer-by-Layer Data Flow

### 1. LANDING Layer (Raw Ingestion)

**Purpose**: Receive raw data from external sources with minimal transformation

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                            LANDING LAYER FLOW                                  │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   BATCH LOADING (Sales, Products, Customers)                                  │
│   ─────────────────────────────────────────                                   │
│                                                                                │
│   ┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│   │   S3     │───▶│  External    │───▶│   COPY INTO  │───▶│   LANDING    │   │
│   │  Bucket  │    │    Stage     │    │   command    │    │    tables    │   │
│   └──────────┘    └──────────────┘    └──────────────┘    └──────────────┘   │
│                                                                                │
│   Example:                                                                     │
│   s3://retail-data/sales/2024/01/sales_20240115.csv                          │
│                    ↓                                                          │
│   @LANDING.SALES_STAGE/2024/01/sales_20240115.csv                            │
│                    ↓                                                          │
│   COPY INTO LANDING.RAW_SALES FROM @SALES_STAGE                              │
│                    ↓                                                          │
│   LANDING.RAW_SALES (with _METADATA columns)                                 │
│                                                                                │
│   ───────────────────────────────────────────────────────────────────────    │
│                                                                                │
│   STREAMING LOADING (Clickstream Events)                                      │
│   ──────────────────────────────────────                                      │
│                                                                                │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐           │
│   │ Kinesis  │───▶│ Firehose │───▶│   S3     │───▶│   Snowpipe   │           │
│   │ Stream   │    │          │    │ Landing  │    │  (auto)      │           │
│   └──────────┘    └──────────┘    └──────────┘    └──────────────┘           │
│                                           │                │                   │
│                                           ▼                ▼                   │
│                                    ┌──────────┐    ┌──────────────┐           │
│                                    │   SNS    │───▶│   LANDING    │           │
│                                    │  Topic   │    │   tables     │           │
│                                    └──────────┘    └──────────────┘           │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Key Components**:
- **External Stages**: Point to S3 locations
- **File Formats**: Define how to parse CSV/JSON/Parquet
- **Snowpipe**: Serverless auto-ingestion
- **External Tables**: Query S3 without loading (optional)

**Tables in LANDING schema**:
```sql
RAW_SALES           -- CSV sales transactions
RAW_PRODUCTS        -- JSON product catalog
RAW_CUSTOMERS       -- Parquet customer profiles
RAW_CLICKSTREAM     -- JSON clickstream events
RAW_INVENTORY       -- CDC inventory updates
```

---

### 2. STAGING Layer (Operational Data Store)

**Purpose**: Cleanse, validate, and standardize data; track changes via Streams

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                            STAGING LAYER FLOW                                  │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   ┌──────────────┐         ┌──────────────┐         ┌──────────────┐          │
│   │   LANDING    │────────▶│  Validation  │────────▶│   STAGING    │          │
│   │    tables    │         │   & Cleanse  │         │    tables    │          │
│   └──────────────┘         └──────────────┘         └──────────────┘          │
│                                    │                        │                  │
│                                    ▼                        ▼                  │
│                            ┌──────────────┐         ┌──────────────┐          │
│                            │   REJECTED   │         │   STREAMS    │          │
│                            │    rows      │         │  (CDC track) │          │
│                            └──────────────┘         └──────────────┘          │
│                                                                                │
│   TRANSFORMATION RULES:                                                        │
│   ─────────────────────                                                       │
│   1. Data type casting (strings → dates, numbers)                            │
│   2. Null handling (defaults, coalesce)                                       │
│   3. Standardization (uppercase, trim, format)                               │
│   4. Deduplication (ROW_NUMBER with partition)                               │
│   5. Validation (regex, range checks, referential)                           │
│                                                                                │
│   CHANGE DATA CAPTURE (via Streams):                                          │
│   ───────────────────────────────────                                         │
│                                                                                │
│   ┌────────────────────────────────────────────────────────────────────┐      │
│   │  STAGING.STG_CUSTOMERS                                              │      │
│   │  ┌─────────────────────────────────────────────────────────────┐   │      │
│   │  │ customer_id | name  | email           | segment   | city    │   │      │
│   │  │ 1001        | John  | john@email.com  | Premium   | NYC     │   │      │
│   │  │ 1002        | Jane  | jane@email.com  | Standard  | LA      │   │      │
│   │  └─────────────────────────────────────────────────────────────┘   │      │
│   └────────────────────────────────────────────────────────────────────┘      │
│                              │                                                 │
│                              ▼                                                 │
│   ┌────────────────────────────────────────────────────────────────────┐      │
│   │  STAGING.STG_CUSTOMERS_STREAM (automatically tracks changes)       │      │
│   │  ┌─────────────────────────────────────────────────────────────┐   │      │
│   │  │ customer_id | name  | email    | METADATA$ACTION | ISUPDATE │   │      │
│   │  │ 1001        | John  | new@mail | INSERT          | FALSE    │   │      │
│   │  │ 1002        | Janet | jane@... | INSERT          | TRUE     │   │      │
│   │  └─────────────────────────────────────────────────────────────┘   │      │
│   └────────────────────────────────────────────────────────────────────┘      │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Staging Tables**:
```sql
STG_SALES           -- Validated sales with proper types
STG_PRODUCTS        -- Parsed JSON with flat structure
STG_CUSTOMERS       -- Customer master with dedup
STG_CLICKSTREAM     -- Enriched events with session IDs
STG_INVENTORY       -- Latest inventory positions
```

**Streams (CDC)**:
```sql
STG_SALES_STREAM        -- Captures INSERT/UPDATE/DELETE
STG_CUSTOMERS_STREAM    -- For SCD Type 2 processing
STG_INVENTORY_STREAM    -- For inventory fact updates
```

---

### 3. CURATED Layer (Data Warehouse)

**Purpose**: Dimensional model (Star Schema) for analytics

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                            CURATED LAYER FLOW                                  │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   DIMENSION LOADING                                                            │
│   ─────────────────                                                           │
│                                                                                │
│   ┌──────────────┐         ┌──────────────┐         ┌──────────────┐          │
│   │   STAGING    │────────▶│    MERGE     │────────▶│  DIMENSIONS  │          │
│   │   STREAM     │         │  (Upsert)    │         │              │          │
│   └──────────────┘         └──────────────┘         └──────────────┘          │
│                                                                                │
│   SCD Type 1 (Overwrite):                                                     │
│   ┌─────────────────────────────────────────────────────────────────────┐     │
│   │  DIM_PRODUCT: Latest product info only                               │     │
│   │                                                                       │     │
│   │  MERGE INTO dim_product t USING stg_products_stream s                │     │
│   │  ON t.product_id = s.product_id                                      │     │
│   │  WHEN MATCHED THEN UPDATE SET t.name = s.name, t.price = s.price    │     │
│   │  WHEN NOT MATCHED THEN INSERT (...)                                  │     │
│   └─────────────────────────────────────────────────────────────────────┘     │
│                                                                                │
│   SCD Type 2 (History):                                                       │
│   ┌─────────────────────────────────────────────────────────────────────┐     │
│   │  DIM_CUSTOMER: Track all historical changes                          │     │
│   │                                                                       │     │
│   │  Step 1: Expire current records that changed                         │     │
│   │  UPDATE dim_customer SET end_date = CURRENT_DATE, is_current = FALSE │     │
│   │  WHERE customer_id IN (SELECT customer_id FROM stream WHERE UPDATE)  │     │
│   │                                                                       │     │
│   │  Step 2: Insert new current records                                  │     │
│   │  INSERT INTO dim_customer (customer_id, ..., start_date, is_current) │     │
│   │  SELECT customer_id, ..., CURRENT_DATE, TRUE FROM stream             │     │
│   └─────────────────────────────────────────────────────────────────────┘     │
│                                                                                │
│   ───────────────────────────────────────────────────────────────────────    │
│                                                                                │
│   FACT LOADING                                                                 │
│   ────────────                                                                │
│                                                                                │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                    │
│   │   STAGING    │───▶│  Lookup      │───▶│  FACT_SALES  │                    │
│   │   STREAM     │    │  Surrogate   │    │              │                    │
│   │              │    │  Keys        │    │              │                    │
│   └──────────────┘    └──────────────┘    └──────────────┘                    │
│                                                                                │
│   Fact Load SQL:                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐     │
│   │  INSERT INTO fact_sales                                              │     │
│   │  SELECT                                                              │     │
│   │      d.date_key,                                                     │     │
│   │      c.customer_key,  -- Lookup from dim_customer (is_current=TRUE) │     │
│   │      p.product_key,   -- Lookup from dim_product                    │     │
│   │      st.store_key,    -- Lookup from dim_store                       │     │
│   │      s.quantity,                                                     │     │
│   │      s.unit_price,                                                   │     │
│   │      s.total_amount                                                  │     │
│   │  FROM stg_sales_stream s                                             │     │
│   │  JOIN dim_date d ON DATE(s.transaction_date) = d.full_date          │     │
│   │  JOIN dim_customer c ON s.customer_id = c.customer_id AND c.is_current │  │
│   │  JOIN dim_product p ON s.product_id = p.product_id                  │     │
│   │  JOIN dim_store st ON s.store_id = st.store_id;                      │     │
│   └─────────────────────────────────────────────────────────────────────┘     │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Curated Tables**:
```sql
-- Dimensions
DIM_CUSTOMER        -- SCD Type 2 (customer_key, ..., start_date, end_date, is_current)
DIM_PRODUCT         -- SCD Type 1 (product_key, product_id, name, category, ...)
DIM_DATE            -- Pre-populated calendar (date_key, full_date, day, month, ...)
DIM_STORE           -- Store/location dimension

-- Facts
FACT_SALES          -- Transactional grain (one row per order line)
FACT_CLICKSTREAM    -- Event grain (one row per click event)
FACT_INVENTORY      -- Snapshot grain (daily inventory positions)
```

---

### 4. ANALYTICS Layer (Data Marts)

**Purpose**: Pre-aggregated views and KPIs for fast BI consumption

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                           ANALYTICS LAYER FLOW                                 │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   ┌──────────────┐                         ┌──────────────────────────────┐   │
│   │   CURATED    │────────────────────────▶│     MATERIALIZED VIEWS       │   │
│   │   (Facts +   │                         │     (Auto-refresh)           │   │
│   │  Dimensions) │                         └──────────────────────────────┘   │
│   └──────────────┘                                       │                     │
│          │                                               │                     │
│          │                                               ▼                     │
│          │         ┌─────────────────────────────────────────────────────┐    │
│          │         │  MV_DAILY_SALES_SUMMARY                             │    │
│          │         │  ┌───────────────────────────────────────────────┐  │    │
│          │         │  │ date | region | category | total_sales | qty  │  │    │
│          │         │  │ 2024-01-15 | East | Electronics | $125,000 | 450│  │    │
│          │         │  │ 2024-01-15 | West | Clothing    | $89,000  | 620│  │    │
│          │         │  └───────────────────────────────────────────────┘  │    │
│          │         └─────────────────────────────────────────────────────┘    │
│          │                                                                     │
│          │         ┌─────────────────────────────────────────────────────┐    │
│          │         │  MV_CUSTOMER_360                                    │    │
│          └────────▶│  ┌───────────────────────────────────────────────┐  │    │
│                    │  │ customer | lifetime_value | last_purchase | ... │  │    │
│                    │  │ C1001    | $15,420        | 2024-01-14    | ... │  │    │
│                    │  └───────────────────────────────────────────────┘  │    │
│                    └─────────────────────────────────────────────────────┘    │
│                                                                                │
│   ───────────────────────────────────────────────────────────────────────    │
│                                                                                │
│   SECURE VIEWS (Row-Level Security)                                           │
│   ─────────────────────────────────                                           │
│                                                                                │
│   ┌─────────────────────────────────────────────────────────────────────┐     │
│   │  VW_SALES_BY_REGION (SECURE)                                        │     │
│   │                                                                      │     │
│   │  CREATE SECURE VIEW vw_sales_by_region AS                           │     │
│   │  SELECT * FROM fact_sales f                                         │     │
│   │  JOIN dim_store s ON f.store_key = s.store_key                     │     │
│   │  WHERE s.region = CURRENT_USER_REGION();  -- UDF for RLS           │     │
│   │                                                                      │     │
│   │  East Manager sees: Only East region sales                          │     │
│   │  West Manager sees: Only West region sales                          │     │
│   │  Admin sees: All sales                                               │     │
│   └─────────────────────────────────────────────────────────────────────┘     │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Analytics Objects**:
```sql
-- Materialized Views (auto-refresh)
MV_DAILY_SALES_SUMMARY      -- Daily aggregates by region/category
MV_CUSTOMER_360             -- Customer lifetime metrics
MV_PRODUCT_PERFORMANCE      -- Product sales analytics

-- Secure Views (row-level security)
VW_SALES_BY_REGION          -- Region-filtered sales
VW_CUSTOMER_SENSITIVE       -- PII masked based on role

-- Aggregate Tables (manually refreshed)
AGG_MONTHLY_REVENUE         -- Monthly revenue by store
AGG_CUSTOMER_COHORTS        -- Cohort analysis data
```

---

## Task Orchestration (ETL DAG)

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                              TASK DAG                                          │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│                        ┌─────────────────┐                                    │
│                        │  ROOT_TASK      │                                    │
│                        │  (Scheduled     │                                    │
│                        │   @hourly)      │                                    │
│                        └────────┬────────┘                                    │
│                                 │                                              │
│              ┌──────────────────┼──────────────────┐                          │
│              │                  │                  │                          │
│              ▼                  ▼                  ▼                          │
│    ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐               │
│    │ LOAD_CUSTOMERS  │ │  LOAD_PRODUCTS  │ │  LOAD_SALES     │               │
│    │ (Staging→Dim)   │ │  (Staging→Dim)  │ │  (Staging→Fact) │               │
│    └────────┬────────┘ └────────┬────────┘ └────────┬────────┘               │
│             │                   │                   │                         │
│             └───────────────────┼───────────────────┘                         │
│                                 │                                              │
│                                 ▼                                              │
│                       ┌─────────────────┐                                     │
│                       │ LOAD_FACT_SALES │                                     │
│                       │ (After all dims)│                                     │
│                       └────────┬────────┘                                     │
│                                │                                               │
│                                ▼                                               │
│                       ┌─────────────────┐                                     │
│                       │ REFRESH_MVS     │                                     │
│                       │ (Update views)  │                                     │
│                       └────────┬────────┘                                     │
│                                │                                               │
│                                ▼                                               │
│                       ┌─────────────────┐                                     │
│                       │ QUALITY_CHECKS  │                                     │
│                       │ (Validation)    │                                     │
│                       └─────────────────┘                                     │
│                                                                                │
│   SCHEDULE: USING CRON '0 * * * *' (every hour)                              │
│   WAREHOUSE: TRANSFORM_WH                                                     │
│   ON FAILURE: Log to TASK_HISTORY, send alert                                │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Task SQL Example**:
```sql
-- Parent task (scheduled)
CREATE OR REPLACE TASK ROOT_TASK
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
    SELECT 1;  -- Dummy query, just triggers children

-- Child task (depends on parent)
CREATE OR REPLACE TASK LOAD_DIM_CUSTOMER
    WAREHOUSE = TRANSFORM_WH
    AFTER ROOT_TASK
AS
    CALL SP_LOAD_DIM_CUSTOMER();

-- Grandchild task (depends on dimensions)
CREATE OR REPLACE TASK LOAD_FACT_SALES
    WAREHOUSE = TRANSFORM_WH
    AFTER LOAD_DIM_CUSTOMER, LOAD_DIM_PRODUCT, LOAD_DIM_STORE
AS
    CALL SP_LOAD_FACT_SALES();
```

---

## Data Quality Checkpoints

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                          DATA QUALITY GATES                                    │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   LANDING → STAGING                                                           │
│   ─────────────────                                                           │
│   □ Schema validation (expected columns exist)                                │
│   □ Data type compatibility                                                   │
│   □ NOT NULL constraints on required fields                                   │
│   □ Duplicate detection (business key uniqueness)                            │
│                                                                                │
│   STAGING → CURATED                                                           │
│   ──────────────────                                                          │
│   □ Referential integrity (FK exists in dimension)                           │
│   □ Business rule validation (amounts > 0, valid dates)                      │
│   □ Range checks (quantities, percentages)                                   │
│   □ Completeness checks (% null acceptable)                                  │
│                                                                                │
│   CURATED → ANALYTICS                                                         │
│   ─────────────────────                                                       │
│   □ Aggregation reconciliation (fact sum = summary sum)                      │
│   □ Time series continuity (no gaps in daily data)                           │
│   □ Cross-table consistency (customer count matches)                         │
│                                                                                │
│   QUALITY METRICS TABLE:                                                      │
│   ┌──────────────────────────────────────────────────────────────────────┐   │
│   │  check_name          | layer    | status | error_count | run_time   │   │
│   │  null_customer_id    | staging  | PASS   | 0           | 2024-01-15 │   │
│   │  orphan_products     | curated  | FAIL   | 23          | 2024-01-15 │   │
│   │  agg_reconciliation  | analytics| PASS   | 0           | 2024-01-15 │   │
│   └──────────────────────────────────────────────────────────────────────┘   │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Monitoring & Alerting

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                         MONITORING POINTS                                      │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   PIPELINE MONITORING                                                          │
│   ───────────────────                                                         │
│   • Task execution history (success/failure rates)                            │
│   • Snowpipe load status (files loaded, errors)                              │
│   • Stream staleness (time since last consume)                               │
│   • Data freshness (max timestamp in tables)                                 │
│                                                                                │
│   PERFORMANCE MONITORING                                                       │
│   ──────────────────────                                                      │
│   • Query execution times (p50, p95, p99)                                    │
│   • Warehouse utilization (queuing, spilling)                                │
│   • Cache hit rates (result cache, warehouse cache)                          │
│   • Clustering depth (for clustered tables)                                  │
│                                                                                │
│   COST MONITORING                                                              │
│   ────────────────                                                            │
│   • Daily credit consumption by warehouse                                     │
│   • Storage growth trends                                                     │
│   • Snowpipe credit usage                                                    │
│   • Resource monitor thresholds                                               │
│                                                                                │
│   ALERTING                                                                     │
│   ────────                                                                    │
│   • Task failure → Email notification                                        │
│   • Data quality failure → Slack webhook                                     │
│   • Cost threshold exceeded → SMS alert                                      │
│   • SLA breach (data not fresh) → PagerDuty                                 │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Interview Discussion Points

### Pipeline Design Decisions

**Q: Why use separate schemas (Landing/Staging/Curated/Analytics)?**
```
A: Separation of concerns and data governance:
- LANDING: Raw data for debugging and reprocessing
- STAGING: Cleansed data with CDC tracking (Transient = cost savings)
- CURATED: Business-modeled data with full Time Travel
- ANALYTICS: Performance-optimized for consumption

Also enables different retention policies and access controls per layer.
```

**Q: Why use Streams instead of timestamp-based incremental loads?**
```
A: Streams provide:
1. Automatic tracking of INSERT/UPDATE/DELETE
2. Exactly-once processing guarantee
3. No need to manage watermarks or timestamps
4. Works with any table, including those without audit columns
5. Transactional consistency with consuming query
```

**Q: How do you handle late-arriving data?**
```
A: Multiple strategies:
1. Stream-based processing handles any order automatically
2. For SCD Type 2, update effective dates retroactively
3. For facts, upsert based on business key
4. Time Travel allows reprocessing from any point
```

**Q: How do you ensure data quality in the pipeline?**
```
A: Multi-layer approach:
1. File format validation on load (ON_ERROR = 'CONTINUE')
2. Staging validation procedures (reject bad rows)
3. Pre-load checks before dimension/fact inserts
4. Post-load reconciliation (counts, sums match)
5. Automated alerts on quality failures
```
