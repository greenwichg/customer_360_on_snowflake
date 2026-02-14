/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - USER DEFINED FUNCTIONS (UDFs)
================================================================================
Purpose: Reusable functions for transformations and calculations
Concepts: SQL UDFs, JavaScript UDFs, UDTFs (table functions)

Interview Points:
- UDFs encapsulate reusable logic
- SQL UDFs are faster, JavaScript UDFs more flexible
- UDTFs return tables (multiple rows)
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;

-- =============================================================================
-- SECTION 1: SQL SCALAR UDFs
-- =============================================================================

-- Calculate age from date of birth
CREATE OR REPLACE FUNCTION UDF_CALCULATE_AGE(dob DATE)
RETURNS INTEGER
LANGUAGE SQL
AS
$$
    DATEDIFF('year', dob, CURRENT_DATE()) -
    CASE WHEN MONTH(CURRENT_DATE()) < MONTH(dob)
         OR (MONTH(CURRENT_DATE()) = MONTH(dob) AND DAY(CURRENT_DATE()) < DAY(dob))
         THEN 1 ELSE 0 END
$$;
-- Usage: SELECT UDF_CALCULATE_AGE('1990-05-15');

-- Classify customer by lifetime value
CREATE OR REPLACE FUNCTION UDF_CUSTOMER_VALUE_TIER(lifetime_value DECIMAL(12,2))
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE
        WHEN lifetime_value >= 10000 THEN 'HIGH_VALUE'
        WHEN lifetime_value >= 5000 THEN 'MEDIUM_VALUE'
        WHEN lifetime_value >= 1000 THEN 'LOW_VALUE'
        ELSE 'NEW_CUSTOMER'
    END
$$;

-- Calculate discount percentage
CREATE OR REPLACE FUNCTION UDF_CALC_DISCOUNT_PCT(gross_amount DECIMAL, net_amount DECIMAL)
RETURNS DECIMAL(5,2)
LANGUAGE SQL
AS
$$
    CASE WHEN gross_amount > 0
         THEN ROUND((1 - (net_amount / gross_amount)) * 100, 2)
         ELSE 0
    END
$$;

-- Mask email for display
CREATE OR REPLACE FUNCTION UDF_MASK_EMAIL(email VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE WHEN email IS NULL THEN NULL
         WHEN POSITION('@' IN email) > 2
         THEN SUBSTRING(email, 1, 2) || '***' || SUBSTRING(email, POSITION('@' IN email))
         ELSE '***@***'
    END
$$;
-- Usage: SELECT UDF_MASK_EMAIL('john.smith@email.com'); -- 'jo***@email.com'

-- =============================================================================
-- SECTION 2: JAVASCRIPT UDFs
-- =============================================================================

-- Parse and validate JSON safely
CREATE OR REPLACE FUNCTION UDF_SAFE_JSON_PARSE(json_string VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    try {
        return JSON.parse(JSON_STRING);
    } catch (e) {
        return null;
    }
$$;

-- Calculate Levenshtein distance (fuzzy matching)
CREATE OR REPLACE FUNCTION UDF_LEVENSHTEIN(s1 VARCHAR, s2 VARCHAR)
RETURNS INTEGER
LANGUAGE JAVASCRIPT
AS
$$
    if (S1 === null || S2 === null) return null;
    var m = S1.length, n = S2.length;
    if (m === 0) return n;
    if (n === 0) return m;

    var d = [];
    for (var i = 0; i <= m; i++) d[i] = [i];
    for (var j = 0; j <= n; j++) d[0][j] = j;

    for (var i = 1; i <= m; i++) {
        for (var j = 1; j <= n; j++) {
            var cost = S1[i-1] === S2[j-1] ? 0 : 1;
            d[i][j] = Math.min(
                d[i-1][j] + 1,
                d[i][j-1] + 1,
                d[i-1][j-1] + cost
            );
        }
    }
    return d[m][n];
$$;
-- Usage: SELECT UDF_LEVENSHTEIN('hello', 'hallo'); -- 1

-- =============================================================================
-- SECTION 3: TABLE FUNCTIONS (UDTFs)
-- =============================================================================

-- Date range generator
CREATE OR REPLACE FUNCTION UDTF_DATE_RANGE(start_date DATE, end_date DATE)
RETURNS TABLE (date_value DATE)
LANGUAGE SQL
AS
$$
    SELECT DATEADD('day', SEQ4(), start_date)::DATE AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', start_date, end_date) + 1))
$$;
-- Usage: SELECT * FROM TABLE(UDTF_DATE_RANGE('2024-01-01', '2024-01-10'));

-- Split string into rows
CREATE OR REPLACE FUNCTION UDTF_SPLIT_STRING(input_string VARCHAR, delimiter VARCHAR)
RETURNS TABLE (part_number INTEGER, part_value VARCHAR)
LANGUAGE SQL
AS
$$
    SELECT
        ROW_NUMBER() OVER (ORDER BY INDEX) AS part_number,
        VALUE::VARCHAR AS part_value
    FROM LATERAL FLATTEN(INPUT => SPLIT(input_string, delimiter))
$$;

-- =============================================================================
-- SECTION 4: SECURE UDFs (For Row-Level Security)
-- =============================================================================

-- Get current user's region (for RLS)
CREATE OR REPLACE SECURE FUNCTION UDF_GET_USER_REGION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT COALESCE(
        (SELECT region FROM METADATA.USER_REGION_MAPPING
         WHERE user_name = CURRENT_USER()),
        'ALL'  -- Default for admins
    )
$$;

-- Check if user can access region
CREATE OR REPLACE SECURE FUNCTION UDF_CAN_ACCESS_REGION(data_region VARCHAR)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    SELECT UDF_GET_USER_REGION() = 'ALL' OR UDF_GET_USER_REGION() = data_region
$$;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Why use UDFs instead of inline SQL?
A1: Reusability, consistency, easier maintenance, cleaner SQL.

Q2: SQL vs JavaScript UDFs performance?
A2: SQL UDFs are optimized by query planner and run faster.
    JavaScript UDFs have overhead but offer more flexibility.

Q3: What are SECURE functions?
A3: Definition is hidden from users without OWNERSHIP privilege.
    Used for security-sensitive logic like RLS helper functions.
*/
