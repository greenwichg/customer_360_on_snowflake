/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - FILE FORMATS
================================================================================
Purpose: Define reusable file formats for different data sources
Concepts: CSV, JSON, Parquet, Avro formats with various options

Interview Points:
- File formats define how to parse incoming data
- Can be defined at stage level or used in COPY commands
- Named formats are reusable and easier to maintain
- Options vary by format type (CSV vs JSON vs Parquet)
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA LANDING;
USE WAREHOUSE LOADING_WH;

-- =============================================================================
-- SECTION 1: CSV FILE FORMATS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 Standard CSV Format (with header)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT CSV_STANDARD
    TYPE = 'CSV'
    COMPRESSION = AUTO                    -- Auto-detect compression
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1                       -- Skip header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'    -- Handle quoted fields
    TRIM_SPACE = TRUE                     -- Remove leading/trailing whitespace
    NULL_IF = ('NULL', 'null', '', '\\N', 'NA', 'N/A')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Allow variable columns
    ESCAPE_UNENCLOSED_FIELD = NONE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    COMMENT = 'Standard CSV format with header row';

-- -----------------------------------------------------------------------------
-- 1.2 CSV Format (no header)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT CSV_NO_HEADER
    TYPE = 'CSV'
    COMPRESSION = AUTO
    FIELD_DELIMITER = ','
    SKIP_HEADER = 0                       -- No header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMMENT = 'CSV format without header row';

-- -----------------------------------------------------------------------------
-- 1.3 CSV Format (pipe-delimited)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT CSV_PIPE_DELIMITED
    TYPE = 'CSV'
    COMPRESSION = AUTO
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', '')
    COMMENT = 'Pipe-delimited format (common in legacy systems)';

-- -----------------------------------------------------------------------------
-- 1.4 CSV Format (tab-delimited / TSV)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT TSV_FORMAT
    TYPE = 'CSV'
    COMPRESSION = AUTO
    FIELD_DELIMITER = '\t'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', '')
    COMMENT = 'Tab-separated values format';

-- =============================================================================
-- SECTION 2: JSON FILE FORMATS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 Standard JSON Format (newline-delimited JSON - NDJSON)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT JSON_STANDARD
    TYPE = 'JSON'
    COMPRESSION = AUTO
    STRIP_OUTER_ARRAY = FALSE             -- Each line is separate JSON object
    STRIP_NULL_VALUES = FALSE             -- Preserve nulls
    IGNORE_UTF8_ERRORS = TRUE             -- Handle encoding issues
    ALLOW_DUPLICATE = FALSE               -- Reject duplicate keys
    ENABLE_OCTAL = FALSE
    COMMENT = 'Standard JSON format (newline-delimited)';

-- -----------------------------------------------------------------------------
-- 2.2 JSON Array Format
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT JSON_ARRAY
    TYPE = 'JSON'
    COMPRESSION = AUTO
    STRIP_OUTER_ARRAY = TRUE              -- File contains JSON array
    STRIP_NULL_VALUES = FALSE
    COMMENT = 'JSON format for files containing a JSON array';

-- -----------------------------------------------------------------------------
-- 2.3 JSON Format (strict)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT JSON_STRICT
    TYPE = 'JSON'
    COMPRESSION = AUTO
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE            -- Fail on encoding errors
    ALLOW_DUPLICATE = FALSE
    COMMENT = 'Strict JSON format - fails on errors';

-- =============================================================================
-- SECTION 3: PARQUET FILE FORMAT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 Standard Parquet Format
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT PARQUET_STANDARD
    TYPE = 'PARQUET'
    COMPRESSION = SNAPPY                  -- Snappy compression (common default)
    BINARY_AS_TEXT = TRUE                 -- Convert binary to text
    COMMENT = 'Standard Parquet format with Snappy compression';

-- -----------------------------------------------------------------------------
-- 3.2 Parquet Format (auto compression detection)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT PARQUET_AUTO
    TYPE = 'PARQUET'
    COMPRESSION = AUTO                    -- Auto-detect compression
    COMMENT = 'Parquet format with automatic compression detection';

-- =============================================================================
-- SECTION 4: AVRO FILE FORMAT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 Standard Avro Format
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT AVRO_STANDARD
    TYPE = 'AVRO'
    COMPRESSION = AUTO
    COMMENT = 'Standard Avro format';

-- =============================================================================
-- SECTION 5: ORC FILE FORMAT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 5.1 Standard ORC Format
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT ORC_STANDARD
    TYPE = 'ORC'
    TRIM_SPACE = TRUE
    COMMENT = 'Standard ORC format';

-- =============================================================================
-- SECTION 6: XML FILE FORMAT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 6.1 Standard XML Format
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT XML_STANDARD
    TYPE = 'XML'
    COMPRESSION = AUTO
    STRIP_OUTER_ELEMENT = TRUE            -- Remove root element
    COMMENT = 'Standard XML format';

-- =============================================================================
-- SECTION 7: VERIFY FILE FORMATS
-- =============================================================================

-- Show all file formats
SHOW FILE FORMATS IN SCHEMA LANDING;

-- Describe a specific format
DESC FILE FORMAT CSV_STANDARD;

-- Test file format with validation
-- SELECT $1, $2, $3
-- FROM @INTERNAL_LANDING_STAGE/sales/
-- (FILE_FORMAT => 'CSV_STANDARD')
-- LIMIT 5;

-- =============================================================================
-- SECTION 8: FILE FORMAT OPTIONS REFERENCE
-- =============================================================================
/*
CSV OPTIONS:
- COMPRESSION: AUTO, GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE, NONE
- FIELD_DELIMITER: Single character (default: ',')
- RECORD_DELIMITER: '\n', '\r', '\r\n'
- SKIP_HEADER: Number of header rows to skip
- FIELD_OPTIONALLY_ENCLOSED_BY: Quote character for fields
- NULL_IF: List of strings to treat as NULL
- EMPTY_FIELD_AS_NULL: TRUE/FALSE
- ERROR_ON_COLUMN_COUNT_MISMATCH: TRUE/FALSE
- TRIM_SPACE: TRUE/FALSE
- ESCAPE: Escape character for enclosed fields
- ESCAPE_UNENCLOSED_FIELD: For unenclosed fields
- DATE_FORMAT: 'AUTO' or specific format
- TIMESTAMP_FORMAT: 'AUTO' or specific format
- ENCODING: UTF8 (default), UTF16, etc.

JSON OPTIONS:
- COMPRESSION: AUTO, GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE, NONE
- STRIP_OUTER_ARRAY: TRUE/FALSE
- STRIP_NULL_VALUES: TRUE/FALSE
- IGNORE_UTF8_ERRORS: TRUE/FALSE
- ALLOW_DUPLICATE: TRUE/FALSE
- ENABLE_OCTAL: TRUE/FALSE
- DATE_FORMAT: 'AUTO' or specific format
- TIMESTAMP_FORMAT: 'AUTO' or specific format

PARQUET OPTIONS:
- COMPRESSION: AUTO, SNAPPY, LZO, GZIP, BROTLI, ZSTD, NONE
- BINARY_AS_TEXT: TRUE/FALSE
- USE_LOGICAL_TYPE: TRUE/FALSE (for dates/timestamps)

AVRO OPTIONS:
- COMPRESSION: AUTO, DEFLATE, SNAPPY, ZSTD, NONE
*/

-- =============================================================================
-- INTERVIEW QUESTIONS & ANSWERS
-- =============================================================================
/*
Q1: What's the difference between stage-level and named file formats?
A1:
    STAGE-LEVEL: Defined inline with stage, can't be reused
    NAMED: Created separately, reusable across stages and COPY commands
    Best practice: Use named formats for consistency and maintainability

Q2: How do you handle dates in different formats?
A2:
    Option 1: Use DATE_FORMAT = 'AUTO' for common formats
    Option 2: Specify exact format: DATE_FORMAT = 'YYYY-MM-DD'
    Option 3: Load as VARCHAR, then parse in staging with TRY_TO_DATE()

    Common formats:
    - 'YYYY-MM-DD' (ISO)
    - 'MM/DD/YYYY' (US)
    - 'DD-MON-YYYY' (Oracle style)

Q3: What happens with ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE?
A3:
    When FALSE:
    - Extra columns in file are ignored
    - Missing columns get NULL values
    - Load continues without errors

    When TRUE:
    - File must have exact column count
    - Mismatch causes load failure
    - Use for strict data validation

Q4: How do you handle NULL values in source files?
A4:
    Use NULL_IF option to specify strings that should become NULL:
    NULL_IF = ('NULL', 'null', '', 'N/A', '\\N', 'None')

    Combined with EMPTY_FIELD_AS_NULL = TRUE:
    - Empty strings become NULL
    - Specified strings become NULL
    - Actual data is preserved

Q5: Why use PARQUET over CSV?
A5:
    PARQUET advantages:
    - Columnar format (faster for analytical queries)
    - Built-in compression (smaller files)
    - Schema embedded in file
    - Better performance for large datasets
    - Supports predicate pushdown (filter before loading)

    CSV advantages:
    - Human readable
    - Simpler tooling
    - Easier debugging
    - Better for small files or streaming

Q6: How do you debug file format issues?
A6:
    1. Use VALIDATION_MODE = 'RETURN_ERRORS' in COPY
    2. Query staged files directly: SELECT $1, $2 FROM @stage LIMIT 5
    3. Check for encoding issues: IGNORE_UTF8_ERRORS
    4. Verify delimiter matches actual file
    5. Check for hidden characters (BOM, invisible unicode)
*/

-- =============================================================================
-- SECTION 9: GRANT PRIVILEGES
-- =============================================================================

GRANT USAGE ON FILE FORMAT CSV_STANDARD TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT CSV_NO_HEADER TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT CSV_PIPE_DELIMITED TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT TSV_FORMAT TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT JSON_STANDARD TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT JSON_ARRAY TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT JSON_STRICT TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT PARQUET_STANDARD TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT PARQUET_AUTO TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT AVRO_STANDARD TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT ORC_STANDARD TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON FILE FORMAT XML_STANDARD TO ROLE RETAIL_ENGINEER;
