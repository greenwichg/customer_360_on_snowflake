/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - ENCRYPTION BEST PRACTICES
================================================================================
Purpose: Document and demonstrate encryption capabilities in Snowflake
Concepts: Encryption at rest, encryption in transit, Tri-Secret Secure, key management

Interview Points:
- Snowflake encrypts all data at rest automatically (AES-256)
- All data in transit is encrypted (TLS 1.2+)
- Tri-Secret Secure provides customer-managed keys
- Periodic key rotation is automatic
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_ANALYTICS_DB;

-- =============================================================================
-- SECTION 1: SNOWFLAKE'S DEFAULT ENCRYPTION
-- =============================================================================

/*
Snowflake provides encryption by DEFAULT with no configuration needed:

ENCRYPTION AT REST:
- All data stored in micro-partitions is encrypted using AES-256
- Encryption keys are managed by Snowflake (hierarchical key model)
- Keys are automatically rotated annually
- Applies to: tables, internal stages, result cache, metadata

ENCRYPTION IN TRANSIT:
- All connections use TLS 1.2 (minimum)
- Client-to-Snowflake: TLS encrypted
- Snowflake-to-cloud storage: TLS encrypted
- Internal communication: TLS encrypted

KEY HIERARCHY:
    Root Key (HSM-protected)
        └── Account Master Key (AMK)
            └── Table Master Key (TMK)
                └── File Encryption Key (FEK)
                    └── Micro-partition data
*/

-- =============================================================================
-- SECTION 2: VERIFY ENCRYPTION SETTINGS
-- =============================================================================

-- Check account encryption settings
SHOW PARAMETERS LIKE '%ENCRYPT%' IN ACCOUNT;

-- Check if periodic rekeying is enabled
SHOW PARAMETERS LIKE 'PERIODIC_DATA_REKEYING' IN ACCOUNT;

-- Enable periodic rekeying (Enterprise Edition)
-- ALTER ACCOUNT SET PERIODIC_DATA_REKEYING = TRUE;

-- =============================================================================
-- SECTION 3: ENCRYPT EXTERNAL STAGE DATA
-- =============================================================================

-- When loading from S3, ensure encryption is configured:

-- Option 1: SSE-S3 (Server-Side Encryption with S3-managed keys)
CREATE OR REPLACE STAGE ENCRYPTED_S3_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/'
    ENCRYPTION = (TYPE = 'AWS_SSE_S3')
    COMMENT = 'S3 stage with SSE-S3 encryption';

-- Option 2: SSE-KMS (Server-Side Encryption with KMS key)
CREATE OR REPLACE STAGE ENCRYPTED_KMS_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/landing/'
    ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = 'aws/key-id-here')
    COMMENT = 'S3 stage with SSE-KMS encryption';

-- Option 3: Client-Side Encryption (CSE)
-- CREATE OR REPLACE STAGE CSE_STAGE
--     URL = 's3://your-retail-data-bucket/encrypted/'
--     ENCRYPTION = (TYPE = 'AWS_CSE' MASTER_KEY = 'base64-encoded-key')
--     COMMENT = 'S3 stage with client-side encryption';

-- =============================================================================
-- SECTION 4: ENCRYPT INTERNAL STAGE DATA
-- =============================================================================

-- Internal stages are automatically encrypted by Snowflake
-- When unloading data, you can specify encryption for the output:

-- Unload with encryption to internal stage
-- COPY INTO @INTERNAL_STAGE/export/
-- FROM CURATED.FACT_SALES
-- FILE_FORMAT = (TYPE = 'CSV')
-- ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL');  -- Default for internal stages

-- Unload to external stage with encryption
-- COPY INTO @ENCRYPTED_S3_STAGE/export/
-- FROM CURATED.FACT_SALES
-- FILE_FORMAT = (TYPE = 'PARQUET')
-- HEADER = TRUE;

-- =============================================================================
-- SECTION 5: TRI-SECRET SECURE (Enterprise)
-- =============================================================================

/*
Tri-Secret Secure provides an additional layer of security by combining:
1. Snowflake-managed encryption key
2. Customer-managed key (in your cloud KMS)
3. Composite master key derived from both

This ensures neither Snowflake nor the cloud provider alone can access data.

SETUP STEPS (AWS):
1. Create a KMS key in your AWS account
2. Grant Snowflake's AWS account access to the KMS key
3. Contact Snowflake Support to enable Tri-Secret Secure
4. Provide the KMS key ARN to Snowflake

Key Policy for AWS KMS:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowSnowflakeAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::SNOWFLAKE_AWS_ACCOUNT:root"
            },
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:DescribeKey"
            ],
            "Resource": "*"
        }
    ]
}
*/

-- =============================================================================
-- SECTION 6: DATA UNLOADING ENCRYPTION
-- =============================================================================

-- When exporting data, ensure encryption is maintained:

-- Create encrypted export stage
CREATE OR REPLACE STAGE ENCRYPTED_EXPORT_STAGE
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    URL = 's3://your-retail-data-bucket/exports/'
    ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = 'aws/key-id-here')
    FILE_FORMAT = (TYPE = 'PARQUET')
    COMMENT = 'Encrypted export stage for data extracts';

-- Example: Export aggregated data (encrypted)
-- COPY INTO @ENCRYPTED_EXPORT_STAGE/monthly_summary/
-- FROM (
--     SELECT region, month_name, year_number, net_revenue
--     FROM AGG_MONTHLY_SALES
-- )
-- OVERWRITE = TRUE
-- HEADER = TRUE;

-- =============================================================================
-- SECTION 7: AUDIT ENCRYPTION COMPLIANCE
-- =============================================================================

-- Verify all stages use encryption
SELECT
    STAGE_NAME,
    STAGE_TYPE,
    STAGE_URL,
    STAGE_OWNER
FROM INFORMATION_SCHEMA.STAGES
WHERE STAGE_SCHEMA IS NOT NULL
ORDER BY STAGE_NAME;

-- Check data access patterns for compliance
SELECT
    USER_NAME,
    QUERY_TYPE,
    QUERY_TEXT,
    START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%COPY INTO%@%'
AND START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC
LIMIT 50;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: How does Snowflake handle encryption by default?
A1: - All data at rest: AES-256 encryption (automatic)
    - All data in transit: TLS 1.2+ (automatic)
    - Hierarchical key model: Root → Account → Table → File keys
    - Automatic key rotation (annually)
    - No configuration required for default encryption

Q2: What is Tri-Secret Secure?
A2: - Combines customer-managed key + Snowflake-managed key
    - Creates a composite master key
    - Neither party alone can decrypt data
    - Requires Enterprise Edition or higher
    - Customer controls their key in their own KMS
    - If customer revokes their key, data becomes inaccessible

Q3: How do you handle encryption for data loading/unloading?
A3: Loading:
    - S3: SSE-S3, SSE-KMS, or client-side encryption
    - Azure: SSE with platform-managed or customer-managed keys
    - Internal stages: Snowflake encryption automatic

    Unloading:
    - Specify encryption type in COPY INTO command
    - Use SSE-KMS for sensitive exports
    - Internal stages use SNOWFLAKE_FULL encryption by default

Q4: What is periodic rekeying?
A4: - Snowflake automatically re-encrypts data with new keys
    - Ensures compliance with key rotation policies
    - Runs in the background with no downtime
    - Available in Enterprise Edition
    - Additional cost for the re-encryption compute

Q5: Can Snowflake employees access customer data?
A5: No. Snowflake's architecture ensures:
    - Customer data is encrypted at rest with customer-specific keys
    - Snowflake personnel do not have access to encryption keys
    - With Tri-Secret Secure, even Snowflake cannot access data
    - SOC 2 Type II and other certifications verify this
*/
