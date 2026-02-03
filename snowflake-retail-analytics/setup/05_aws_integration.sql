/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - AWS INTEGRATION SETUP
================================================================================
Purpose: Configure secure integration between Snowflake and AWS S3
Concepts: Storage Integration, IAM roles, external stages

Interview Points:
- Storage Integration uses IAM roles (no access keys in Snowflake)
- Creates trust relationship between Snowflake and AWS
- Single integration can be used for multiple stages
- More secure than storing AWS credentials
================================================================================
*/

USE ROLE ACCOUNTADMIN;  -- Required for storage integration creation
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA LANDING;

-- =============================================================================
-- SECTION 1: STORAGE INTEGRATION
-- =============================================================================
/*
Storage Integration creates a secure connection to AWS S3 without storing
credentials. It uses Snowflake's AWS IAM user and external ID for
cross-account access.

Steps:
1. Create storage integration in Snowflake
2. Note the AWS IAM user ARN and External ID
3. Create IAM role in AWS with trust policy
4. Update integration if needed
*/

CREATE OR REPLACE STORAGE INTEGRATION S3_RETAIL_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-retail-role'
    STORAGE_ALLOWED_LOCATIONS = (
        's3://your-retail-data-bucket/landing/',
        's3://your-retail-data-bucket/staging/',
        's3://your-retail-data-bucket/archive/'
    )
    STORAGE_BLOCKED_LOCATIONS = (
        's3://your-retail-data-bucket/sensitive/'
    )
    COMMENT = 'Integration for retail data S3 bucket';

-- Get the AWS IAM user ARN and External ID for trust policy
-- Run this and copy values to AWS IAM role trust policy
DESC INTEGRATION S3_RETAIL_INTEGRATION;

/*
Expected output includes:
- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::123456789012:user/xxxx
- STORAGE_AWS_EXTERNAL_ID: ABC123_SFCRole=xxx
Use these values in the AWS IAM trust policy (see Section 2)
*/

-- =============================================================================
-- SECTION 2: AWS IAM ROLE CONFIGURATION (Reference)
-- =============================================================================
/*
Create this IAM role in AWS Console or via CloudFormation/Terraform:

IAM Role Name: snowflake-retail-role

Trust Policy (AssumeRole):
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<STORAGE_AWS_IAM_USER_ARN from DESC INTEGRATION>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION>"
                }
            }
        }
    ]
}

Permissions Policy (S3 Access):
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::your-retail-data-bucket",
                "arn:aws:s3:::your-retail-data-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::your-retail-data-bucket/staging/*"
        }
    ]
}
*/

-- =============================================================================
-- SECTION 3: EXTERNAL STAGES
-- =============================================================================
/*
External stages point to S3 locations using the storage integration.
They define the URL path and default file format.
*/

-- -----------------------------------------------------------------------------
-- 3.1 Sales Data Stage (CSV)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE SALES_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/sales/'
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    )
    COMMENT = 'Stage for sales transaction CSV files';

-- -----------------------------------------------------------------------------
-- 3.2 Products Data Stage (JSON)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE PRODUCTS_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/products/'
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = FALSE
        COMPRESSION = AUTO
    )
    COMMENT = 'Stage for product catalog JSON files';

-- -----------------------------------------------------------------------------
-- 3.3 Customers Data Stage (Parquet)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE CUSTOMERS_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/customers/'
    FILE_FORMAT = (
        TYPE = 'PARQUET'
        COMPRESSION = SNAPPY
    )
    COMMENT = 'Stage for customer Parquet files';

-- -----------------------------------------------------------------------------
-- 3.4 Clickstream Data Stage (JSON - for Snowpipe)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE CLICKSTREAM_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/clickstream/'
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = FALSE
    )
    COMMENT = 'Stage for clickstream JSON files (Snowpipe auto-ingest)';

-- =============================================================================
-- SECTION 4: ALTERNATIVE - INTERNAL STAGE (No AWS Required)
-- =============================================================================
/*
For local testing without AWS, use internal (Snowflake-managed) stages.
Data is stored in Snowflake's internal storage.
*/

-- Create internal stage for local testing
CREATE OR REPLACE STAGE INTERNAL_LANDING_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    COMMENT = 'Internal stage for local data loading (no AWS required)';

-- Upload files to internal stage using SnowSQL or Web UI:
-- PUT file://C:/data/sales_transactions.csv @INTERNAL_LANDING_STAGE/sales/;
-- PUT file://C:/data/products.json @INTERNAL_LANDING_STAGE/products/;
-- PUT file://C:/data/customers.csv @INTERNAL_LANDING_STAGE/customers/;

-- =============================================================================
-- SECTION 5: VERIFY STAGE SETUP
-- =============================================================================

-- Show all stages
SHOW STAGES IN SCHEMA LANDING;

-- List files in a stage
LIST @SALES_STAGE;
-- LIST @INTERNAL_LANDING_STAGE/sales/;

-- Preview data from stage (without loading)
-- SELECT $1, $2, $3 FROM @SALES_STAGE/sales_2024.csv LIMIT 10;

-- =============================================================================
-- SECTION 6: S3 EVENT NOTIFICATION FOR SNOWPIPE (Reference)
-- =============================================================================
/*
For Snowpipe auto-ingest, configure S3 event notifications:

1. Create SNS Topic:
   - Name: snowflake-retail-notifications
   - Add Snowflake's SQS ARN as subscriber (from Snowpipe definition)

2. Configure S3 Event Notification:
   - Event types: s3:ObjectCreated:*
   - Prefix: landing/clickstream/
   - Destination: SNS topic

3. S3 Bucket Policy (allow SNS publishing):
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sns:Publish",
            "Resource": "arn:aws:sns:region:account-id:snowflake-retail-notifications",
            "Condition": {
                "ArnLike": {"aws:SourceArn": "arn:aws:s3:::your-retail-data-bucket"}
            }
        }
    ]
}
*/

-- =============================================================================
-- SECTION 7: NOTIFICATION INTEGRATION FOR SNOWPIPE
-- =============================================================================

-- Create notification integration (for auto-ingest Snowpipe)
CREATE OR REPLACE NOTIFICATION INTEGRATION S3_NOTIFICATION_INT
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AWS_SNS
    DIRECTION = INBOUND
    AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:snowflake-retail-notifications'
    AWS_SNS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-sns-role'
    COMMENT = 'SNS notification integration for Snowpipe auto-ingest';

-- Get the Snowflake IAM user ARN for SNS subscription
-- DESC NOTIFICATION INTEGRATION S3_NOTIFICATION_INT;

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: Why use Storage Integration instead of access keys?
A1:
    - More secure: No credentials stored in Snowflake
    - Uses IAM roles with temporary credentials
    - External ID prevents confused deputy attacks
    - Easier credential rotation (just update IAM role)
    - Centralized access control in AWS IAM

Q2: What's the difference between External and Internal stages?
A2:
    EXTERNAL STAGE:
    - Points to cloud storage (S3, Azure Blob, GCS)
    - Data stays in your cloud account
    - You manage storage costs separately
    - Can use existing data lakes

    INTERNAL STAGE:
    - Data stored in Snowflake-managed storage
    - Simpler setup (no cloud integration needed)
    - Included in Snowflake storage costs
    - Good for small datasets or temp uploads

Q3: How does Snowpipe auto-ingest work?
A3:
    1. Files land in S3 bucket
    2. S3 triggers event notification
    3. Notification goes to SNS topic
    4. SNS delivers to Snowflake SQS queue
    5. Snowpipe detects new files
    6. Serverless compute loads files
    7. Files marked as processed (won't reload)

    Latency: Typically 1-2 minutes from file upload

Q4: How do you troubleshoot stage access issues?
A4:
    1. Check STORAGE_ALLOWED_LOCATIONS includes the path
    2. Verify IAM role trust policy has correct external ID
    3. Check IAM permissions policy allows required actions
    4. Use LIST @stage to verify connectivity
    5. Check for IP restrictions (VPC endpoints, bucket policies)
    6. Verify integration is ENABLED

Q5: Can multiple stages use the same storage integration?
A5: Yes! One integration can be used for multiple stages.
    This is the recommended pattern:
    - One integration per AWS account or security boundary
    - Multiple stages pointing to different S3 paths
    - Reduces setup complexity and credential management

Q6: How do you handle different file formats in the same S3 path?
A6:
    Option 1: Use stage-level file format, load specific patterns
        COPY INTO table FROM @stage PATTERN='.*\.csv';

    Option 2: Override file format in COPY command
        COPY INTO table FROM @stage FILE_FORMAT = (TYPE = 'JSON');

    Option 3: Create multiple stages pointing to same path
        with different default file formats
*/

-- =============================================================================
-- SECTION 8: TEST CONNECTIVITY
-- =============================================================================

-- Test listing files (replace with actual stage after AWS setup)
-- LIST @SALES_STAGE;

-- Test reading data from stage
-- SELECT * FROM @SALES_STAGE/sample.csv (FILE_FORMAT => 'CSV_FORMAT') LIMIT 5;

-- For local testing with internal stage:
-- PUT file:///path/to/sales_transactions.csv @INTERNAL_LANDING_STAGE/sales/;
-- LIST @INTERNAL_LANDING_STAGE/sales/;

-- Grant stage usage to engineer role
GRANT USAGE ON INTEGRATION S3_RETAIL_INTEGRATION TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE SALES_STAGE TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE PRODUCTS_STAGE TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE CUSTOMERS_STAGE TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE CLICKSTREAM_STAGE TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON STAGE INTERNAL_LANDING_STAGE TO ROLE RETAIL_ENGINEER;
