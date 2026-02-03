# Real-Time 360° Analytics Platform on Snowflake

A comprehensive, production-ready Snowflake data engineering project demonstrating end-to-end data pipelines from AWS to Snowflake. This project covers ALL major Snowflake concepts and is designed for deep learning and interview preparation.

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Snowflake Concepts Covered](#snowflake-concepts-covered)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Quick Start Guide](#quick-start-guide)
- [Layer-by-Layer Implementation](#layer-by-layer-implementation)
- [Cost Estimation](#cost-estimation)
- [Interview Preparation](#interview-preparation)
- [Troubleshooting](#troubleshooting)

---

## Project Overview

### Business Context
Build a **multi-channel retail analytics platform** that provides:
- **Real-time Customer 360°**: Unified view of customer interactions across all touchpoints
- **Sales Analytics**: Revenue trends, product performance, regional analysis
- **Inventory Management**: Stock levels, reorder alerts, supply chain optimization
- **Clickstream Analytics**: User behavior, conversion funnels, session analysis

### Technical Goals
- Demonstrate **end-to-end data pipeline** from AWS to Snowflake
- Implement **batch, micro-batch, and near-real-time** loading patterns
- Build **dimensional data model** with SCD Type 1 and Type 2
- Apply **enterprise security** with RBAC, masking, and row-level security
- Optimize **performance** with clustering, caching, and materialized views

### Data Sources
| Source | Type | Volume | Frequency |
|--------|------|--------|-----------|
| AWS S3 (Sales) | Batch CSV | 1M+ records | Daily |
| AWS S3 (Products) | Batch JSON | 10K records | Weekly |
| AWS S3 (Customers) | Batch Parquet | 100K records | Daily |
| AWS Kinesis (Clickstream) | Streaming JSON | 5M+ events | Real-time |
| AWS RDS MySQL (Inventory) | CDC | Continuous | Near real-time |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES (AWS)                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐       │
│  │   AWS S3    │    │  AWS RDS    │    │ AWS Kinesis │    │  External   │       │
│  │  (Files)    │    │  (MySQL)    │    │ (Streaming) │    │   APIs      │       │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘       │
└─────────┼──────────────────┼──────────────────┼──────────────────┼──────────────┘
          │                  │                  │                  │
          ▼                  ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           SNOWFLAKE PLATFORM                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        CLOUD SERVICES LAYER                              │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │    │
│  │  │ Security │ │ Metadata │ │ Query    │ │ Resource │ │ Access   │      │    │
│  │  │ Manager  │ │ Manager  │ │ Optimizer│ │ Monitor  │ │ Control  │      │    │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘      │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        COMPUTE LAYER (Virtual Warehouses)                │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │    │
│  │  │ LOADING_WH   │  │ TRANSFORM_WH │  │ ANALYTICS_WH │                   │    │
│  │  │ (X-Small)    │  │ (Small)      │  │ (Medium)     │                   │    │
│  │  │ Batch Loads  │  │ ETL Tasks    │  │ BI Queries   │                   │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        STORAGE LAYER (Shared)                            │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐    │    │
│  │  │                    RETAIL_ANALYTICS_DB                           │    │    │
│  │  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐    │    │    │
│  │  │  │  LANDING  │  │  STAGING  │  │  CURATED  │  │ ANALYTICS │    │    │    │
│  │  │  │  (RAW)    │──│  (ODS)    │──│  (DWH)    │──│  (MARTS)  │    │    │    │
│  │  │  └───────────┘  └───────────┘  └───────────┘  └───────────┘    │    │    │
│  │  └─────────────────────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              DATA PIPELINE FLOW                                   │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐    │
│  │   S3    │────▶│ LANDING │────▶│ STAGING │────▶│ CURATED │────▶│ANALYTICS│    │
│  │ (RAW)   │     │ (EXT)   │     │ (ODS)   │     │ (DWH)   │     │ (MART)  │    │
│  └─────────┘     └─────────┘     └─────────┘     └─────────┘     └─────────┘    │
│       │              │                │                │               │         │
│       │              │                │                │               │         │
│       ▼              ▼                ▼                ▼               ▼         │
│  ┌─────────┐   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    │
│  │External │   │ Snowpipe │    │ Streams  │    │  Tasks   │    │   Mat.   │    │
│  │ Stage   │   │ (Auto)   │    │  (CDC)   │    │  (ETL)   │    │  Views   │    │
│  └─────────┘   └──────────┘    └──────────┘    └──────────┘    └──────────┘    │
│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Snowflake Concepts Covered

### ✅ Architecture & Foundations
| Concept | File | Description |
|---------|------|-------------|
| Three-Layer Architecture | `architecture/` | Storage, Compute, Cloud Services |
| Virtual Warehouses | `setup/01_warehouses.sql` | Sizing, auto-suspend, scaling |
| Database Types | `setup/02_databases_schemas.sql` | Permanent, Transient, Temporary |
| Multi-Schema Design | `setup/02_databases_schemas.sql` | Landing → Staging → Curated → Analytics |

### ✅ Data Loading & Integration
| Concept | File | Description |
|---------|------|-------------|
| Storage Integration | `setup/05_aws_integration.sql` | Secure S3 access |
| External Stages | `landing-layer/external_stages.sql` | S3 connection points |
| File Formats | `landing-layer/file_formats.sql` | CSV, JSON, Parquet, Avro |
| COPY INTO | `staging-layer/copy_commands.sql` | Bulk loading with options |
| Snowpipe | `landing-layer/snowpipe_setup.sql` | Auto-ingest from S3 |
| External Tables | `landing-layer/external_tables.sql` | Query S3 without loading |

### ✅ Data Transformation
| Concept | File | Description |
|---------|------|-------------|
| Streams | `staging-layer/streams.sql` | Change Data Capture |
| Tasks | `curated-layer/transformations/tasks_orchestration.sql` | Scheduled workflows |
| Stored Procedures | `curated-layer/transformations/stored_procedures.sql` | Business logic |
| UDFs | `curated-layer/transformations/udfs.sql` | SQL & JavaScript functions |
| Materialized Views | `analytics-layer/materialized_views.sql` | Pre-aggregated data |

### ✅ Advanced Features
| Concept | File | Description |
|---------|------|-------------|
| Time Travel | `advanced-features/time_travel_examples.sql` | Historical queries |
| Zero-Copy Cloning | `advanced-features/zero_copy_cloning.sql` | Instant copies |
| Data Sharing | `advanced-features/data_sharing_setup.sql` | Secure sharing |
| Data Sampling | `testing/data_validation_tests.sql` | Testing patterns |

### ✅ Security & Governance
| Concept | File | Description |
|---------|------|-------------|
| RBAC | `setup/04_roles_users.sql` | Role hierarchy |
| Row-Level Security | `security/row_access_policies.sql` | Data filtering |
| Column Masking | `security/masking_policies.sql` | PII protection |
| Network Policies | `security/network_policies.sql` | IP restrictions |

### ✅ Performance & Optimization
| Concept | File | Description |
|---------|------|-------------|
| Clustering Keys | `advanced-features/clustering_keys.sql` | Query performance |
| Search Optimization | `advanced-features/search_optimization.sql` | Point lookups |
| Query Tuning | `monitoring/query_performance.sql` | Analysis patterns |
| Caching | `interview-prep/common_questions.md` | Result & Warehouse cache |

---

## Project Structure

```
snowflake-retail-analytics/
├── README.md                          # This file - comprehensive documentation
├── architecture/
│   ├── architecture_diagram.md        # Visual architecture with Mermaid
│   └── data_flow.md                   # End-to-end data flow explanation
├── setup/
│   ├── 01_warehouses.sql              # Virtual warehouses for different workloads
│   ├── 02_databases_schemas.sql       # Database hierarchy (Permanent/Transient)
│   ├── 03_resource_monitors.sql       # Cost controls and credit alerts
│   ├── 04_roles_users.sql             # RBAC with role hierarchy
│   └── 05_aws_integration.sql         # Storage Integration for S3
├── data-sources/
│   ├── sample-data/                   # Ready-to-load sample files
│   │   ├── sales_transactions.csv     # 1M+ sales records
│   │   ├── products.json              # 10K product catalog
│   │   ├── customers.csv              # 100K customer profiles
│   │   └── clickstream_events.json    # 5M+ user events
│   └── aws-setup/
│       ├── s3_bucket_structure.md     # S3 folder organization guide
│       ├── iam_policies.json          # IAM roles for Snowflake access
│       └── kinesis_setup.md           # Kinesis configuration
├── landing-layer/
│   ├── external_stages.sql            # S3 external stages
│   ├── file_formats.sql               # CSV, JSON, Parquet, Avro formats
│   ├── external_tables.sql            # Query S3 directly (schema-on-read)
│   └── snowpipe_setup.sql             # Auto-ingest with S3 notifications
├── staging-layer/
│   ├── schema_definition.sql          # Transient tables (cost-effective ODS)
│   ├── copy_commands.sql              # Load from S3 with error handling
│   ├── streams.sql                    # CDC streams for incremental processing
│   └── data_validation.sql            # Data quality checks
├── curated-layer/
│   ├── dimensional_model/
│   │   ├── dim_customer.sql           # SCD Type 2 (history tracking)
│   │   ├── dim_product.sql            # SCD Type 1 (overwrite)
│   │   ├── dim_date.sql               # Pre-populated date dimension
│   │   ├── dim_store.sql              # Store/location dimension
│   │   └── fact_sales.sql             # Transactional fact table
│   ├── transformations/
│   │   ├── tasks_orchestration.sql    # Task DAG with dependencies
│   │   ├── stored_procedures.sql      # Complex business logic
│   │   └── udfs.sql                   # Custom SQL & JavaScript functions
│   └── data_quality/
│       └── validation_procedures.sql  # Automated quality checks
├── analytics-layer/
│   ├── materialized_views.sql         # Pre-aggregated summaries
│   ├── secure_views.sql               # Row-level security implementation
│   ├── aggregations.sql               # Summary/aggregate tables
│   └── kpi_metrics.sql                # Business KPI calculations
├── advanced-features/
│   ├── time_travel_examples.sql       # Query historical data, undrop
│   ├── zero_copy_cloning.sql          # Dev/test environment creation
│   ├── data_sharing_setup.sql         # Share data with partners
│   ├── clustering_keys.sql            # Large table optimization
│   └── search_optimization.sql        # Point lookup acceleration
├── security/
│   ├── masking_policies.sql           # Dynamic data masking for PII
│   ├── row_access_policies.sql        # Row-level security policies
│   ├── network_policies.sql           # IP whitelist/blacklist
│   └── encryption.sql                 # Encryption best practices
├── monitoring/
│   ├── query_performance.sql          # Query analysis and tuning
│   ├── cost_monitoring.sql            # Credit usage tracking
│   ├── data_lineage.sql               # Track data flow
│   └── alerts.sql                     # Automated notifications
├── testing/
│   ├── data_validation_tests.sql      # Data quality assertions
│   ├── performance_tests.sql          # Load and query benchmarks
│   └── integration_tests.sql          # End-to-end pipeline tests
└── interview-prep/
    ├── common_questions.md            # 50+ interview Q&A
    ├── code_snippets.md               # Reusable SQL patterns
    └── troubleshooting.md             # Common issues & solutions
```

---

## Data Model

### Star Schema Design

```
                              ┌─────────────────┐
                              │   dim_date      │
                              ├─────────────────┤
                              │ date_key (PK)   │
                              │ full_date       │
                              │ day_of_week     │
                              │ month           │
                              │ quarter         │
                              │ year            │
                              │ is_weekend      │
                              │ is_holiday      │
                              └────────┬────────┘
                                       │
┌─────────────────┐           ┌────────┴────────┐           ┌─────────────────┐
│  dim_customer   │           │   fact_sales    │           │   dim_product   │
├─────────────────┤           ├─────────────────┤           ├─────────────────┤
│ customer_key(PK)│◄──────────┤ customer_key(FK)│───────────▶│ product_key(PK)│
│ customer_id     │           │ product_key(FK) │           │ product_id      │
│ first_name      │           │ store_key (FK)  │           │ product_name    │
│ last_name       │           │ date_key (FK)   │           │ category        │
│ email           │           │ order_id        │           │ subcategory     │
│ phone           │           │ quantity        │           │ brand           │
│ segment         │           │ unit_price      │           │ unit_cost       │
│ city            │           │ discount_pct    │           │ unit_price      │
│ state           │           │ total_amount    │           │ is_active       │
│ effective_date  │           │ transaction_ts  │           └─────────────────┘
│ end_date        │           └────────┬────────┘
│ is_current      │                    │
│ (SCD Type 2)    │           ┌────────┴────────┐
└─────────────────┘           │   dim_store     │
                              ├─────────────────┤
                              │ store_key (PK)  │
                              │ store_id        │
                              │ store_name      │
                              │ store_type      │
                              │ address         │
                              │ city            │
                              │ state           │
                              │ region          │
                              │ manager_name    │
                              │ open_date       │
                              └─────────────────┘
```

### Additional Fact Tables

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        fact_clickstream                                  │
├─────────────────────────────────────────────────────────────────────────┤
│ event_id (PK) | customer_key (FK) | date_key (FK) | session_id         │
│ event_type | page_url | referrer_url | device_type | browser           │
│ ip_address | duration_seconds | timestamp                               │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                        fact_inventory                                    │
├─────────────────────────────────────────────────────────────────────────┤
│ inventory_id (PK) | product_key (FK) | store_key (FK) | date_key (FK)  │
│ quantity_on_hand | quantity_reserved | reorder_point | reorder_qty     │
│ last_restock_date | snapshot_timestamp                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start Guide

### Prerequisites
- Snowflake account (Trial account works - 30 days, $400 credits)
- AWS account with S3 access (optional - for full integration)
- SQL client (Snowflake Web UI, DBeaver, or VS Code)

### Step 1: Initial Setup (10 minutes)
```sql
-- Run these scripts in order:
-- 1. Create warehouses for different workloads
@setup/01_warehouses.sql

-- 2. Create database and schema hierarchy
@setup/02_databases_schemas.sql

-- 3. Set up resource monitors for cost control
@setup/03_resource_monitors.sql

-- 4. Create roles and users with RBAC
@setup/04_roles_users.sql

-- 5. (Optional) Set up AWS integration
@setup/05_aws_integration.sql
```

### Step 2: Load Sample Data (5 minutes)
```sql
-- Create file formats
@landing-layer/file_formats.sql

-- Create external stages (use internal if no AWS)
@landing-layer/external_stages.sql

-- Load data into staging
@staging-layer/schema_definition.sql
@staging-layer/copy_commands.sql
```

### Step 3: Build Data Warehouse (10 minutes)
```sql
-- Create dimensional model
@curated-layer/dimensional_model/dim_date.sql
@curated-layer/dimensional_model/dim_customer.sql
@curated-layer/dimensional_model/dim_product.sql
@curated-layer/dimensional_model/dim_store.sql
@curated-layer/dimensional_model/fact_sales.sql

-- Set up transformations
@curated-layer/transformations/udfs.sql
@curated-layer/transformations/stored_procedures.sql
@curated-layer/transformations/tasks_orchestration.sql
```

### Step 4: Configure Analytics (5 minutes)
```sql
-- Create materialized views and aggregations
@analytics-layer/materialized_views.sql
@analytics-layer/aggregations.sql
@analytics-layer/kpi_metrics.sql

-- Set up secure views for row-level security
@analytics-layer/secure_views.sql
```

### Step 5: Apply Security (5 minutes)
```sql
-- Apply masking policies for PII
@security/masking_policies.sql

-- Apply row access policies
@security/row_access_policies.sql
```

### Step 6: Verify Setup
```sql
-- Run validation tests
@testing/data_validation_tests.sql

-- Check pipeline status
@monitoring/query_performance.sql
```

---

## Cost Estimation

### Snowflake Trial Account
| Resource | Allocation | Cost |
|----------|------------|------|
| Credits | 400 credits | Free (30 days) |
| Storage | Unlimited | Included |

### Estimated Credit Usage (This Project)

| Activity | Warehouse | Hours | Credits/Hour | Total Credits |
|----------|-----------|-------|--------------|---------------|
| Initial Setup | XS | 0.5 | 1 | 0.5 |
| Data Loading | XS | 1 | 1 | 1 |
| Transformations | S | 2 | 2 | 4 |
| Analytics Queries | M | 2 | 4 | 8 |
| Testing | XS | 1 | 1 | 1 |
| **Total** | - | 6.5 | - | **14.5** |

**Note**: With 400 free credits, you can run this project multiple times!

### Cost Optimization Tips
1. Use `AUTO_SUSPEND = 60` (1 minute) for dev warehouses
2. Use Transient tables for staging (no Fail-safe = lower storage cost)
3. Use Resource Monitors to prevent runaway queries
4. Leverage result caching for repeated queries

---

## Interview Preparation

See the `/interview-prep` folder for comprehensive materials:

### Quick Reference - Top Interview Topics

#### 1. Architecture Questions
- **Q**: Explain Snowflake's three-layer architecture
- **A**: Storage (S3/Azure Blob/GCS), Compute (Virtual Warehouses), Cloud Services (metadata, security, optimization)

#### 2. Performance Questions
- **Q**: How do you optimize a slow query in Snowflake?
- **A**: Check Query Profile, add clustering keys, use materialized views, right-size warehouse, leverage caching

#### 3. Security Questions
- **Q**: How do you implement PII protection?
- **A**: Dynamic Data Masking policies, Row Access Policies, Secure Views, Role-based Access Control

#### 4. Cost Questions
- **Q**: How do you control Snowflake costs?
- **A**: Resource Monitors, warehouse auto-suspend, right-sizing, query optimization, transient tables

### Project-Specific Interview Points
- "In my project, I implemented SCD Type 2 for customer dimension using MERGE statements..."
- "I set up Snowpipe for continuous ingestion from S3 with automatic scaling..."
- "I used Zero-Copy Cloning to create development environments instantly..."

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `COPY command skipped` | Check FILE_FORMAT settings, especially DATE_FORMAT |
| `Access denied to S3` | Verify Storage Integration IAM role |
| `Task not running` | Ensure warehouse is running and task is RESUMED |
| `Stream has no data` | Initial stream capture needs DML first |
| `Query timeout` | Increase warehouse size or optimize query |

### Useful Diagnostic Queries
```sql
-- Check warehouse credit usage
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP());

-- Find slow queries
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE EXECUTION_TIME > 60000  -- > 1 minute
ORDER BY START_TIME DESC;

-- Check Snowpipe status
SELECT SYSTEM$PIPE_STATUS('my_pipe');
```

---

## Contributing

This is a learning project. Feel free to:
- Add more sample data scenarios
- Implement additional Snowflake features
- Improve documentation
- Add more interview questions

---

## License

MIT License - Use freely for learning and interview preparation.

---

## Author

Built for mastering Snowflake data engineering and acing interviews!

**Happy Learning! ❄️**
