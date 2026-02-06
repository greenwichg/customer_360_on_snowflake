/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DIM_PRODUCT (SCD TYPE 1)
================================================================================
Purpose: Product dimension with overwrite updates (SCD Type 1)
Concepts: Slowly Changing Dimension Type 1, surrogate keys, MERGE pattern

Interview Points:
- SCD Type 1 overwrites existing values (no history)
- Simpler than Type 2 but loses change history
- Suitable when historical attribute values are not needed
- MERGE statement handles both inserts and updates efficiently
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: CREATE DIMENSION TABLE
-- =============================================================================

CREATE OR REPLACE TABLE DIM_PRODUCT (
    -- Surrogate key (auto-generated)
    product_key INTEGER AUTOINCREMENT START 1 INCREMENT 1,

    -- Business key (natural key from source)
    product_id VARCHAR(50) NOT NULL,

    -- Product attributes
    product_name VARCHAR(500),
    description VARCHAR(2000),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(200),

    -- Pricing
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    profit_margin DECIMAL(5,2),

    -- Physical attributes
    weight_kg DECIMAL(8,3),
    dimensions VARCHAR(100),

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    launch_date DATE,
    discontinue_date DATE,

    -- Semi-structured attributes (flexible product metadata)
    attributes VARIANT,

    -- Audit columns
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key),
    CONSTRAINT uq_product_id UNIQUE (product_id)
)
COMMENT = 'Product dimension with SCD Type 1 (overwrite on change)';

-- =============================================================================
-- SECTION 2: SCD TYPE 1 LOADING PROCEDURE (MERGE)
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_PRODUCT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
BEGIN
    -- Check if stream has data
    IF (SYSTEM$STREAM_HAS_DATA('STAGING.STG_PRODUCTS_STREAM') = FALSE) THEN
        RETURN 'No changes to process';
    END IF;

    -- MERGE: Insert new products, update existing ones (SCD Type 1)
    MERGE INTO DIM_PRODUCT target
    USING (
        SELECT
            product_id,
            product_name,
            description,
            category,
            subcategory,
            brand,
            unit_cost,
            unit_price,
            CASE
                WHEN unit_price > 0 THEN ROUND((unit_price - unit_cost) / unit_price * 100, 2)
                ELSE 0
            END AS profit_margin,
            weight_kg,
            dimensions,
            is_active,
            launch_date,
            discontinue_date,
            attributes
        FROM STAGING.STG_PRODUCTS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
        AND dq_is_valid = TRUE
    ) source
    ON target.product_id = source.product_id

    -- Update existing products (SCD Type 1: overwrite)
    WHEN MATCHED THEN UPDATE SET
        product_name = source.product_name,
        description = source.description,
        category = source.category,
        subcategory = source.subcategory,
        brand = source.brand,
        unit_cost = source.unit_cost,
        unit_price = source.unit_price,
        profit_margin = source.profit_margin,
        weight_kg = source.weight_kg,
        dimensions = source.dimensions,
        is_active = source.is_active,
        launch_date = source.launch_date,
        discontinue_date = source.discontinue_date,
        attributes = source.attributes,
        updated_timestamp = CURRENT_TIMESTAMP()

    -- Insert new products
    WHEN NOT MATCHED THEN INSERT (
        product_id, product_name, description, category, subcategory, brand,
        unit_cost, unit_price, profit_margin, weight_kg, dimensions,
        is_active, launch_date, discontinue_date, attributes
    )
    VALUES (
        source.product_id, source.product_name, source.description,
        source.category, source.subcategory, source.brand,
        source.unit_cost, source.unit_price, source.profit_margin,
        source.weight_kg, source.dimensions,
        source.is_active, source.launch_date, source.discontinue_date,
        source.attributes
    );

    RETURN 'DIM_PRODUCT load completed (SCD Type 1 MERGE)';
END;
$$;

-- =============================================================================
-- SECTION 3: HELPER VIEWS
-- =============================================================================

-- Active products only
CREATE OR REPLACE VIEW VW_DIM_PRODUCT_ACTIVE AS
SELECT * FROM DIM_PRODUCT WHERE is_active = TRUE;

-- Product category summary
CREATE OR REPLACE VIEW VW_PRODUCT_CATEGORY_SUMMARY AS
SELECT
    category,
    subcategory,
    COUNT(*) AS product_count,
    AVG(unit_price) AS avg_price,
    AVG(profit_margin) AS avg_margin,
    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_count
FROM DIM_PRODUCT
GROUP BY category, subcategory
ORDER BY category, subcategory;

-- =============================================================================
-- SECTION 4: VERIFY AND GRANT
-- =============================================================================

DESCRIBE TABLE DIM_PRODUCT;

GRANT SELECT ON TABLE DIM_PRODUCT TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE DIM_PRODUCT TO ROLE RETAIL_VIEWER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DIM_PRODUCT TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON TABLE DIM_PRODUCT TO ROLE RETAIL_ADMIN;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: When do you choose SCD Type 1 vs Type 2?
A1: SCD Type 1 (overwrite):
    - When you only care about current values
    - When corrections should replace errors
    - Simpler to implement and maintain
    - Lower storage requirements

    SCD Type 2 (history):
    - When you need historical analysis ("What was the price last month?")
    - When audit trails are required
    - When fact tables need point-in-time accuracy

Q2: Why use MERGE for SCD Type 1?
A2: MERGE handles both INSERT and UPDATE in one statement:
    - Atomic: Both operations succeed or fail together
    - Efficient: Single table scan instead of separate INSERT + UPDATE
    - Idempotent: Safe to re-run without duplicates

Q3: Why store attributes as VARIANT?
A3: Product metadata varies by category (e.g., electronics have specs,
    clothing has sizes). VARIANT allows flexible schema:
    - No ALTER TABLE needed for new attributes
    - Can query with dot notation: attributes:color::STRING
    - Supports nested structures
*/
