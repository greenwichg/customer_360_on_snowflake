# AWS Kinesis Setup for Clickstream Data

## Overview

This document describes the AWS Kinesis configuration for ingesting real-time clickstream events into the Snowflake retail analytics platform. Kinesis Data Firehose delivers events to S3, where Snowpipe automatically loads them into Snowflake.

---

## Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────┐     ┌──────────────┐
│   Web/App    │────▶│  Kinesis Data     │────▶│    S3       │────▶│  Snowpipe    │
│   Events     │     │  Firehose         │     │  Bucket     │     │  (Auto)      │
└──────────────┘     └──────────────────┘     └─────────────┘     └──────────────┘
                            │                        │
                     ┌──────┴──────┐          ┌──────┴──────┐
                     │ Buffer:     │          │ S3 Event    │
                     │ 1 min/1 MB  │          │ Notification│
                     │ Transform   │          │ → SNS → SQS │
                     └─────────────┘          └─────────────┘
```

---

## Step 1: Create Kinesis Data Stream (Optional)

If you need to fan out events to multiple consumers, create a Kinesis Data Stream first.

### AWS CLI
```bash
aws kinesis create-stream \
    --stream-name retail-clickstream \
    --shard-count 2 \
    --region us-east-1
```

### Shard Sizing
| Metric | Per Shard |
|--------|-----------|
| Write capacity | 1 MB/sec or 1,000 records/sec |
| Read capacity | 2 MB/sec |
| Retention | 24 hours (default), up to 365 days |

**For 5M events/day (~58 events/sec):** 1-2 shards is sufficient.

---

## Step 2: Create Kinesis Data Firehose Delivery Stream

Firehose delivers events directly to S3 in batches.

### AWS CLI
```bash
aws firehose create-delivery-stream \
    --delivery-stream-name retail-clickstream-to-s3 \
    --delivery-stream-type DirectPut \
    --s3-destination-configuration '{
        "RoleARN": "arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/firehose-s3-role",
        "BucketARN": "arn:aws:s3:::your-retail-data-bucket",
        "Prefix": "landing/clickstream/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/",
        "ErrorOutputPrefix": "staging/failed/clickstream/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
        "BufferingHints": {
            "SizeInMBs": 1,
            "IntervalInSeconds": 60
        },
        "CompressionFormat": "GZIP",
        "EncryptionConfiguration": {
            "NoEncryptionConfig": "NoEncryption"
        }
    }'
```

### Buffer Configuration

| Setting | Value | Description |
|---------|-------|-------------|
| Buffer Size | 1 MB | Flush when buffer reaches 1 MB |
| Buffer Interval | 60 seconds | Flush every 60 seconds (minimum) |
| Compression | GZIP | Reduces storage and transfer costs |

**Trade-off:** Smaller buffer = lower latency but more S3 files. Larger buffer = fewer files but higher latency.

---

## Step 3: Firehose IAM Role

The Firehose delivery stream needs an IAM role to write to S3.

### Trust Policy
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "firehose.amazonaws.com"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "YOUR_AWS_ACCOUNT_ID"
                }
            }
        }
    ]
}
```

### Permissions Policy
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3Access",
            "Effect": "Allow",
            "Action": [
                "s3:AbortMultipartUpload",
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-retail-data-bucket",
                "arn:aws:s3:::your-retail-data-bucket/*"
            ]
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:PutLogEvents",
                "logs:CreateLogStream"
            ],
            "Resource": "arn:aws:logs:us-east-1:YOUR_AWS_ACCOUNT_ID:log-group:/aws/firehose/retail-clickstream-to-s3:*"
        }
    ]
}
```

---

## Step 4: Clickstream Event Schema

### Event Format (JSON)
```json
{
    "event_id": "evt-uuid-12345",
    "event_type": "page_view",
    "customer_id": "CUST-10001",
    "session_id": "sess-abc-12345",
    "timestamp": "2024-01-15T14:30:00.000Z",
    "page_url": "/products/electronics/laptop-pro",
    "referrer_url": "/search?q=laptop",
    "device_type": "desktop",
    "browser": "Chrome",
    "os": "Windows 10",
    "ip_address": "192.168.1.100",
    "user_agent": "Mozilla/5.0...",
    "geo": {
        "country": "US",
        "state": "CA",
        "city": "San Francisco"
    },
    "properties": {
        "product_id": "PROD-5001",
        "category": "Electronics",
        "price": 999.99,
        "duration_seconds": 45
    }
}
```

### Event Types
| Event Type | Description | Key Properties |
|-----------|-------------|----------------|
| `page_view` | User views a page | page_url, duration |
| `product_view` | User views a product detail page | product_id, category |
| `add_to_cart` | User adds item to cart | product_id, quantity |
| `remove_from_cart` | User removes item from cart | product_id |
| `checkout_start` | User begins checkout | cart_value |
| `purchase` | User completes purchase | order_id, total_amount |
| `search` | User performs search | search_query, result_count |
| `click` | User clicks an element | element_id, element_type |

---

## Step 5: Sending Events to Firehose

### Python SDK Example
```python
import boto3
import json
from datetime import datetime

firehose = boto3.client('firehose', region_name='us-east-1')

def send_clickstream_event(event):
    """Send a single clickstream event to Firehose."""
    response = firehose.put_record(
        DeliveryStreamName='retail-clickstream-to-s3',
        Record={
            'Data': json.dumps(event) + '\n'  # newline-delimited JSON
        }
    )
    return response

def send_batch_events(events):
    """Send a batch of events (up to 500 per call)."""
    records = [{'Data': json.dumps(e) + '\n'} for e in events]
    response = firehose.put_record_batch(
        DeliveryStreamName='retail-clickstream-to-s3',
        Records=records
    )
    return response

# Example usage
event = {
    "event_id": "evt-001",
    "event_type": "page_view",
    "customer_id": "CUST-10001",
    "session_id": "sess-abc-001",
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "page_url": "/products/electronics",
    "device_type": "mobile",
    "browser": "Safari"
}

send_clickstream_event(event)
```

---

## Step 6: Snowflake Integration

Once events land in S3, Snowpipe handles loading automatically.

### Snowpipe Configuration (in Snowflake)
```sql
-- External stage pointing to clickstream S3 path
CREATE OR REPLACE STAGE CLICKSTREAM_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/clickstream/'
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE);

-- Snowpipe for auto-ingest
CREATE OR REPLACE PIPE CLICKSTREAM_PIPE
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:snowflake-retail-notifications'
AS
COPY INTO LANDING.RAW_CLICKSTREAM
FROM @CLICKSTREAM_STAGE
FILE_FORMAT = (TYPE = 'JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### Expected Latency
```
Event → Firehose buffer (60s) → S3 write → SNS notification → Snowpipe load (~60-120s)
Total end-to-end: ~2-4 minutes
```

---

## Monitoring

### CloudWatch Metrics for Firehose
| Metric | Alert Threshold | Description |
|--------|----------------|-------------|
| `IncomingRecords` | < 10/min | Events stopped flowing |
| `DeliveryToS3.DataFreshness` | > 300 sec | Delivery delay |
| `DeliveryToS3.Success` | < 1.0 | Failed deliveries |
| `ThrottledRecords` | > 0 | Shard capacity exceeded |

### CloudWatch Alarm Example
```bash
aws cloudwatch put-metric-alarm \
    --alarm-name "firehose-delivery-delay" \
    --metric-name "DeliveryToS3.DataFreshness" \
    --namespace "AWS/Firehose" \
    --dimensions Name=DeliveryStreamName,Value=retail-clickstream-to-s3 \
    --statistic Maximum \
    --period 300 \
    --threshold 300 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 2 \
    --alarm-actions "arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:ops-alerts"
```

---

## Cost Estimation

| Component | Pricing | Estimated Monthly Cost |
|-----------|---------|----------------------|
| Kinesis Data Firehose | $0.029/GB ingested | ~$15 (500 GB/month) |
| S3 Storage | $0.023/GB | ~$12 (500 GB) |
| S3 PUT requests | $0.005/1000 | ~$5 (1M requests) |
| SNS notifications | $0.50/1M | ~$1 |
| **Total** | | **~$33/month** |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Events not appearing in S3 | Check Firehose delivery stream status in CloudWatch |
| High delivery latency | Reduce buffer interval, check for throttling |
| Malformed JSON in S3 | Ensure newline-delimited format in PutRecord |
| Snowpipe not triggering | Verify SNS topic subscription and S3 event config |
| Duplicate events | Implement idempotent processing with event_id deduplication |
