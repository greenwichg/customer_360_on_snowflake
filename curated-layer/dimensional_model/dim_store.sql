/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DIM_STORE
================================================================================
Purpose: Store/location dimension for regional analysis
Concepts: Dimension table design, geographic hierarchy, store attributes

Interview Points:
- Store dimension enables regional, store-type, and manager-level analysis
- Geographic hierarchy: Store → City → State → Region → Country
- SCD Type 1 approach (overwrite) since store history is less critical
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: CREATE DIMENSION TABLE
-- =============================================================================

CREATE OR REPLACE TABLE DIM_STORE (
    -- Surrogate key
    store_key INTEGER AUTOINCREMENT START 1 INCREMENT 1,

    -- Business key
    store_id VARCHAR(50) NOT NULL,

    -- Store attributes
    store_name VARCHAR(200),
    store_type VARCHAR(50),       -- Flagship, Standard, Outlet, Online
    store_format VARCHAR(50),     -- Large, Medium, Small, Kiosk

    -- Location
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'US',
    region VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),

    -- Operational attributes
    manager_name VARCHAR(200),
    phone VARCHAR(50),
    email VARCHAR(255),
    open_date DATE,
    close_date DATE,
    square_footage INTEGER,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    -- Audit columns
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT pk_dim_store PRIMARY KEY (store_key),
    CONSTRAINT uq_store_id UNIQUE (store_id)
)
COMMENT = 'Store/location dimension for regional and store-level analysis';

-- =============================================================================
-- SECTION 2: LOADING PROCEDURE (SCD TYPE 1 - MERGE)
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_STORE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO DIM_STORE target
    USING (
        SELECT
            store_id,
            store_name,
            store_type,
            store_format,
            address,
            city,
            state,
            postal_code,
            country,
            CASE
                WHEN state IN ('NY', 'NJ', 'PA', 'MA', 'CT', 'RI', 'VT', 'NH', 'ME') THEN 'Northeast'
                WHEN state IN ('CA', 'OR', 'WA', 'NV', 'AZ', 'CO', 'UT', 'HI', 'AK') THEN 'West'
                WHEN state IN ('TX', 'FL', 'GA', 'NC', 'VA', 'SC', 'AL', 'TN', 'LA', 'MS') THEN 'South'
                WHEN state IN ('IL', 'OH', 'MI', 'IN', 'WI', 'MN', 'IA', 'MO', 'KS', 'NE') THEN 'Midwest'
                ELSE 'Other'
            END AS region,
            latitude,
            longitude,
            manager_name,
            phone,
            email,
            open_date,
            close_date,
            square_footage,
            is_active
        FROM STAGING.STG_STORES
        WHERE dq_is_valid = TRUE
    ) source
    ON target.store_id = source.store_id

    WHEN MATCHED THEN UPDATE SET
        store_name = source.store_name,
        store_type = source.store_type,
        store_format = source.store_format,
        address = source.address,
        city = source.city,
        state = source.state,
        postal_code = source.postal_code,
        country = source.country,
        region = source.region,
        latitude = source.latitude,
        longitude = source.longitude,
        manager_name = source.manager_name,
        phone = source.phone,
        email = source.email,
        open_date = source.open_date,
        close_date = source.close_date,
        square_footage = source.square_footage,
        is_active = source.is_active,
        updated_timestamp = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        store_id, store_name, store_type, store_format,
        address, city, state, postal_code, country, region,
        latitude, longitude, manager_name, phone, email,
        open_date, close_date, square_footage, is_active
    )
    VALUES (
        source.store_id, source.store_name, source.store_type, source.store_format,
        source.address, source.city, source.state, source.postal_code,
        source.country, source.region, source.latitude, source.longitude,
        source.manager_name, source.phone, source.email,
        source.open_date, source.close_date, source.square_footage,
        source.is_active
    );

    RETURN 'DIM_STORE load completed';
END;
$$;

-- =============================================================================
-- SECTION 3: HELPER VIEWS
-- =============================================================================

-- Active stores with region summary
CREATE OR REPLACE VIEW VW_STORE_REGIONAL_SUMMARY AS
SELECT
    region,
    store_type,
    COUNT(*) AS store_count,
    SUM(square_footage) AS total_sqft,
    AVG(square_footage) AS avg_sqft,
    MIN(open_date) AS earliest_open,
    MAX(open_date) AS latest_open
FROM DIM_STORE
WHERE is_active = TRUE
GROUP BY region, store_type
ORDER BY region, store_type;

-- =============================================================================
-- SECTION 4: VERIFY AND GRANT
-- =============================================================================

DESCRIBE TABLE DIM_STORE;

GRANT SELECT ON TABLE DIM_STORE TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE DIM_STORE TO ROLE RETAIL_VIEWER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DIM_STORE TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON TABLE DIM_STORE TO ROLE RETAIL_ADMIN;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Why denormalize geographic hierarchy into the store dimension?
A1: Star schema best practice:
    - Avoids snowflake-like joins (City → State → Region)
    - Simplifies queries for analysts
    - Minimal storage overhead for lookup values
    - Enables direct GROUP BY on any level

Q2: When would you use SCD Type 2 for stores?
A2: Use Type 2 if you need to track:
    - Manager changes over time (performance analysis)
    - Store type changes (Standard → Flagship promotion)
    - Store relocations (address changes)
    For most retail analytics, Type 1 is sufficient.
*/
