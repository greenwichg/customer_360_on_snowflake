/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DIM_CUSTOMER (SCD TYPE 2)
================================================================================
Purpose: Customer dimension with full history tracking
Concepts: Slowly Changing Dimension Type 2, surrogate keys, effective dates

Interview Points:
- SCD Type 2 preserves historical changes
- Uses surrogate keys for fact table joins
- effective_date/end_date track version validity
- is_current flag identifies latest version
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: CREATE DIMENSION TABLE
-- =============================================================================

CREATE OR REPLACE TABLE DIM_CUSTOMER (
    -- Surrogate key (auto-generated)
    customer_key INTEGER AUTOINCREMENT START 1 INCREMENT 1,

    -- Business key (natural key from source)
    customer_id VARCHAR(50) NOT NULL,

    -- Dimension attributes
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(10),
    age_group VARCHAR(20),

    -- Business attributes
    registration_date DATE,
    customer_segment VARCHAR(50),
    loyalty_points INTEGER,
    loyalty_tier VARCHAR(20),
    preferred_contact VARCHAR(50),
    is_active BOOLEAN,

    -- Address (denormalized for star schema)
    address_line1 VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    region VARCHAR(50),

    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    record_hash VARCHAR(64),

    -- Audit columns
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
)
CLUSTER BY (is_current, customer_segment)
COMMENT = 'Customer dimension with SCD Type 2 history tracking';

-- Create index-like optimization with clustering
-- ALTER TABLE DIM_CUSTOMER CLUSTER BY (is_current, customer_id);

-- =============================================================================
-- SECTION 2: SCD TYPE 2 LOADING PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_CUSTOMER()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
BEGIN
    -- Check if stream has data
    IF (SYSTEM$STREAM_HAS_DATA('STAGING.STG_CUSTOMERS_STREAM') = FALSE) THEN
        RETURN 'No changes to process';
    END IF;

    -- Step 1: Get new/changed records from stream
    CREATE OR REPLACE TEMPORARY TABLE TEMP_CUSTOMER_CHANGES AS
    SELECT
        customer_id,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        email,
        phone,
        date_of_birth,
        gender,
        CASE
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 25 THEN '18-24'
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 35 THEN '25-34'
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 45 THEN '35-44'
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 55 THEN '45-54'
            WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 65 THEN '55-64'
            ELSE '65+'
        END AS age_group,
        registration_date,
        customer_segment,
        loyalty_points,
        CASE
            WHEN loyalty_points >= 40000 THEN 'PLATINUM'
            WHEN loyalty_points >= 20000 THEN 'GOLD'
            WHEN loyalty_points >= 10000 THEN 'SILVER'
            ELSE 'BRONZE'
        END AS loyalty_tier,
        preferred_contact,
        is_active,
        address_line1,
        city,
        state,
        postal_code,
        country,
        CASE
            WHEN state IN ('NY', 'NJ', 'PA', 'MA', 'CT') THEN 'Northeast'
            WHEN state IN ('CA', 'OR', 'WA', 'NV', 'AZ') THEN 'West'
            WHEN state IN ('TX', 'FL', 'GA', 'NC', 'VA') THEN 'South'
            WHEN state IN ('IL', 'OH', 'MI', 'IN', 'WI') THEN 'Midwest'
            ELSE 'Other'
        END AS region,
        record_hash
    FROM STAGING.STG_CUSTOMERS_STREAM
    WHERE METADATA$ACTION = 'INSERT'
    AND dq_is_valid = TRUE;

    -- Step 2: Expire current records that have changes
    UPDATE DIM_CUSTOMER target
    SET
        end_date = DATEADD('day', -1, CURRENT_DATE()),
        is_current = FALSE,
        updated_timestamp = CURRENT_TIMESTAMP()
    WHERE target.is_current = TRUE
    AND target.customer_id IN (SELECT customer_id FROM TEMP_CUSTOMER_CHANGES)
    AND target.record_hash != (
        SELECT record_hash FROM TEMP_CUSTOMER_CHANGES
        WHERE TEMP_CUSTOMER_CHANGES.customer_id = target.customer_id
    );

    GET_DML_NUM_ROWS_AFFECTED(v_rows_updated);

    -- Step 3: Insert new versions (changes + new customers)
    INSERT INTO DIM_CUSTOMER (
        customer_id, first_name, last_name, full_name, email, phone,
        date_of_birth, gender, age_group, registration_date,
        customer_segment, loyalty_points, loyalty_tier, preferred_contact,
        is_active, address_line1, city, state, postal_code, country, region,
        effective_date, end_date, is_current, record_hash
    )
    SELECT
        src.customer_id, src.first_name, src.last_name, src.full_name,
        src.email, src.phone, src.date_of_birth, src.gender, src.age_group,
        src.registration_date, src.customer_segment, src.loyalty_points,
        src.loyalty_tier, src.preferred_contact, src.is_active,
        src.address_line1, src.city, src.state, src.postal_code,
        src.country, src.region,
        CURRENT_DATE(),  -- effective_date
        NULL,            -- end_date (NULL = current)
        TRUE,            -- is_current
        src.record_hash
    FROM TEMP_CUSTOMER_CHANGES src
    WHERE NOT EXISTS (
        SELECT 1 FROM DIM_CUSTOMER target
        WHERE target.customer_id = src.customer_id
        AND target.is_current = TRUE
        AND target.record_hash = src.record_hash
    );

    GET_DML_NUM_ROWS_AFFECTED(v_rows_inserted);

    RETURN 'SCD Type 2 complete: ' || v_rows_inserted || ' inserted, ' || v_rows_updated || ' expired';
END;
$$;

-- =============================================================================
-- SECTION 3: INITIAL LOAD (Full Load)
-- =============================================================================

-- For initial load, insert all customers as current
-- INSERT INTO DIM_CUSTOMER (
--     customer_id, first_name, last_name, full_name, email, phone,
--     date_of_birth, gender, age_group, registration_date,
--     customer_segment, loyalty_points, loyalty_tier, preferred_contact,
--     is_active, address_line1, city, state, postal_code, country, region,
--     effective_date, end_date, is_current, record_hash
-- )
-- SELECT ... FROM STAGING.STG_CUSTOMERS WHERE dq_is_valid = TRUE;

-- =============================================================================
-- SECTION 4: HELPER VIEWS
-- =============================================================================

-- Current customers only
CREATE OR REPLACE VIEW VW_DIM_CUSTOMER_CURRENT AS
SELECT * FROM DIM_CUSTOMER WHERE is_current = TRUE;

-- Customer history (all versions)
CREATE OR REPLACE VIEW VW_DIM_CUSTOMER_HISTORY AS
SELECT
    customer_key,
    customer_id,
    full_name,
    customer_segment,
    loyalty_tier,
    effective_date,
    end_date,
    is_current,
    DATEDIFF('day', effective_date, COALESCE(end_date, CURRENT_DATE())) AS days_in_version
FROM DIM_CUSTOMER
ORDER BY customer_id, effective_date;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Why use SCD Type 2 for customers?
A1: Customer attributes change (address, segment, loyalty tier).
    Historical analysis needs point-in-time accuracy.
    Example: "What was this customer's segment when they made this purchase?"

Q2: How do you handle the surrogate key?
A2: AUTOINCREMENT generates unique keys. Fact tables join on
    customer_key, not customer_id. This allows multiple versions
    of the same customer in analysis.

Q3: Why is_current flag AND end_date?
A3: Both have purposes:
    - is_current: Fast filter for current records
    - end_date: Enable point-in-time queries (WHERE date BETWEEN effective AND end)
*/
