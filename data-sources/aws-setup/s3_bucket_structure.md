# S3 Bucket Structure for Snowflake Retail Analytics

## Overview

This document outlines the S3 bucket organization for the retail analytics data pipeline. A well-organized bucket structure is critical for efficient data loading, Snowpipe configuration, and access control.

---

## Bucket Structure

```
s3://your-retail-data-bucket/
├── landing/                          # Raw data ingestion zone
│   ├── sales/                        # Sales transaction files
│   │   ├── daily/                    # Daily batch loads
│   │   │   ├── sales_2024_01_15.csv
│   │   │   ├── sales_2024_01_16.csv
│   │   │   └── ...
│   │   └── archive/                  # Processed files moved here
│   │       └── 2024/01/
│   ├── products/                     # Product catalog files
│   │   ├── products_full.json        # Full refresh (weekly)
│   │   └── products_delta.json       # Incremental updates
│   ├── customers/                    # Customer profile files
│   │   ├── daily/
│   │   │   ├── customers_2024_01_15.parquet
│   │   │   └── ...
│   │   └── archive/
│   ├── clickstream/                  # Real-time event data
│   │   ├── year=2024/
│   │   │   ├── month=01/
│   │   │   │   ├── day=15/
│   │   │   │   │   ├── hour=00/
│   │   │   │   │   │   ├── events_00001.json
│   │   │   │   │   │   └── events_00002.json
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── ...
│   └── inventory/                    # Inventory snapshots
│       └── daily/
│           └── inventory_snapshot_2024_01_15.csv
├── staging/                          # Intermediate processing zone
│   ├── transformed/                  # Post-transformation data
│   └── failed/                       # Files that failed validation
├── archive/                          # Long-term storage
│   ├── sales/
│   ├── customers/
│   └── clickstream/
└── sensitive/                        # Restricted access zone (blocked in Snowflake)
    ├── pii_exports/
    └── compliance_reports/
```

---

## Naming Conventions

### File Naming Pattern
```
{source}_{type}_{YYYY}_{MM}_{DD}[_{sequence}].{format}
```

**Examples:**
| File Name | Description |
|-----------|-------------|
| `sales_2024_01_15.csv` | Daily sales for Jan 15, 2024 |
| `products_full.json` | Full product catalog refresh |
| `customers_2024_01_15.parquet` | Daily customer snapshot |
| `events_00001.json` | Clickstream events (Kinesis batch) |

### Partition Keys for Clickstream
Clickstream data uses Hive-style partitioning for efficient querying:
```
year=YYYY/month=MM/day=DD/hour=HH/
```

---

## Lifecycle Policies

Configure S3 lifecycle rules to manage storage costs:

| Path | Rule | Transition | Expiration |
|------|------|-----------|------------|
| `landing/*/archive/` | Move to IA | 30 days → S3-IA | 365 days |
| `landing/clickstream/` | Move to IA | 7 days → S3-IA | 90 days |
| `staging/failed/` | Expire | - | 30 days |
| `archive/` | Move to Glacier | 90 days → Glacier | 730 days |

### Example Lifecycle Configuration (JSON)
```json
{
    "Rules": [
        {
            "ID": "archive-old-landing-data",
            "Status": "Enabled",
            "Filter": { "Prefix": "landing/sales/archive/" },
            "Transitions": [
                { "Days": 30, "StorageClass": "STANDARD_IA" },
                { "Days": 180, "StorageClass": "GLACIER" }
            ],
            "Expiration": { "Days": 730 }
        },
        {
            "ID": "cleanup-failed-staging",
            "Status": "Enabled",
            "Filter": { "Prefix": "staging/failed/" },
            "Expiration": { "Days": 30 }
        }
    ]
}
```

---

## Access Patterns

### Snowflake Integration Points

| S3 Path | Snowflake Stage | Load Method | Frequency |
|---------|----------------|-------------|-----------|
| `landing/sales/daily/` | `SALES_STAGE` | COPY INTO / Snowpipe | Daily |
| `landing/products/` | `PRODUCTS_STAGE` | COPY INTO | Weekly |
| `landing/customers/daily/` | `CUSTOMERS_STAGE` | COPY INTO / Snowpipe | Daily |
| `landing/clickstream/` | `CLICKSTREAM_STAGE` | Snowpipe (auto-ingest) | Real-time |

### Storage Integration Mapping
```sql
-- Allowed locations in STORAGE INTEGRATION
STORAGE_ALLOWED_LOCATIONS = (
    's3://your-retail-data-bucket/landing/',
    's3://your-retail-data-bucket/staging/',
    's3://your-retail-data-bucket/archive/'
)

-- Blocked locations (sensitive data)
STORAGE_BLOCKED_LOCATIONS = (
    's3://your-retail-data-bucket/sensitive/'
)
```

---

## Bucket Policy

### Enable Server-Side Encryption (SSE-S3)
```json
{
    "Rules": [
        {
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "aws:kms",
                "KMSMasterKeyID": "arn:aws:kms:us-east-1:ACCOUNT_ID:key/KEY_ID"
            },
            "BucketKeyEnabled": true
        }
    ]
}
```

### Enable Versioning
Versioning should be enabled to protect against accidental deletions and support data recovery.

### Enable Access Logging
Configure access logging to a separate bucket for audit purposes:
```
s3://your-retail-data-bucket-logs/
```

---

## S3 Event Notifications

Configure event notifications for Snowpipe auto-ingest:

| Event | Prefix Filter | Destination |
|-------|--------------|-------------|
| `s3:ObjectCreated:*` | `landing/clickstream/` | SNS: `snowflake-retail-notifications` |
| `s3:ObjectCreated:*` | `landing/sales/daily/` | SNS: `snowflake-retail-notifications` |
| `s3:ObjectCreated:*` | `landing/customers/daily/` | SNS: `snowflake-retail-notifications` |

---

## Best Practices

1. **Use consistent prefixes** - Aligns with Snowflake stage URL paths
2. **Enable versioning** - Protects against accidental overwrites
3. **Enable encryption** - SSE-S3 or SSE-KMS for data at rest
4. **Set lifecycle policies** - Automate archival and cleanup
5. **Use partitioned paths** - Enables efficient Snowflake partition pruning
6. **Block public access** - Ensure bucket is not publicly accessible
7. **Tag resources** - Use tags for cost allocation and organization
