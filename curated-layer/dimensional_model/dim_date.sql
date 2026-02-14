/*
================================================================================
SNOWFLAKE RETAIL ANALYTICS - DIM_DATE (Pre-populated Calendar)
================================================================================
Purpose: Date dimension with pre-populated calendar attributes
Concepts: Date dimension, fiscal calendar, holiday flags, date hierarchy

Interview Points:
- Date dimension is pre-populated (not loaded from source)
- Integer date_key (YYYYMMDD) enables efficient joins and partitioning
- Includes both calendar and fiscal year attributes
- Holiday and weekend flags enable business day calculations
================================================================================
*/

USE ROLE RETAIL_ENGINEER;
USE DATABASE RETAIL_ANALYTICS_DB;
USE SCHEMA CURATED;
USE WAREHOUSE TRANSFORM_WH;

-- =============================================================================
-- SECTION 1: CREATE DATE DIMENSION TABLE
-- =============================================================================

CREATE OR REPLACE TABLE DIM_DATE (
    -- Primary key (integer format: YYYYMMDD)
    date_key INTEGER NOT NULL,
    full_date DATE NOT NULL,

    -- Day-level attributes
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_short_name VARCHAR(3),
    day_of_month INTEGER,
    day_of_year INTEGER,

    -- Week-level attributes
    week_of_year INTEGER,
    week_start_date DATE,
    week_end_date DATE,

    -- Month-level attributes
    month_number INTEGER,
    month_name VARCHAR(10),
    month_short_name VARCHAR(3),
    month_start_date DATE,
    month_end_date DATE,

    -- Quarter-level attributes
    quarter_number INTEGER,
    quarter_name VARCHAR(10),

    -- Year-level attributes
    year_number INTEGER,

    -- Fiscal calendar (July-June fiscal year)
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_quarter_name VARCHAR(10),

    -- Flags
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100),
    is_business_day BOOLEAN,

    -- Relative flags (useful for filtering)
    is_current_day BOOLEAN,
    is_current_week BOOLEAN,
    is_current_month BOOLEAN,
    is_current_quarter BOOLEAN,
    is_current_year BOOLEAN,

    -- Constraints
    PRIMARY KEY (date_key),
    UNIQUE (full_date)
)
COMMENT = 'Date dimension - pre-populated calendar with fiscal year support';

-- =============================================================================
-- SECTION 2: POPULATE DATE DIMENSION (6 YEARS: 2020-2025)
-- =============================================================================

INSERT INTO DIM_DATE
SELECT
    -- Keys
    TO_NUMBER(TO_CHAR(d.date_value, 'YYYYMMDD')) AS date_key,
    d.date_value AS full_date,

    -- Day
    DAYOFWEEK(d.date_value) AS day_of_week,
    DAYNAME(d.date_value) AS day_name,
    LEFT(DAYNAME(d.date_value), 3) AS day_short_name,
    DAY(d.date_value) AS day_of_month,
    DAYOFYEAR(d.date_value) AS day_of_year,

    -- Week
    WEEKOFYEAR(d.date_value) AS week_of_year,
    DATE_TRUNC('week', d.date_value) AS week_start_date,
    DATEADD('day', 6, DATE_TRUNC('week', d.date_value)) AS week_end_date,

    -- Month
    MONTH(d.date_value) AS month_number,
    MONTHNAME(d.date_value) AS month_name,
    LEFT(MONTHNAME(d.date_value), 3) AS month_short_name,
    DATE_TRUNC('month', d.date_value) AS month_start_date,
    LAST_DAY(d.date_value) AS month_end_date,

    -- Quarter
    QUARTER(d.date_value) AS quarter_number,
    'Q' || QUARTER(d.date_value) AS quarter_name,

    -- Year
    YEAR(d.date_value) AS year_number,

    -- Fiscal year (July = FY start)
    CASE WHEN MONTH(d.date_value) >= 7 THEN YEAR(d.date_value) + 1 ELSE YEAR(d.date_value) END AS fiscal_year,
    CASE
        WHEN MONTH(d.date_value) IN (7,8,9) THEN 1
        WHEN MONTH(d.date_value) IN (10,11,12) THEN 2
        WHEN MONTH(d.date_value) IN (1,2,3) THEN 3
        ELSE 4
    END AS fiscal_quarter,
    'FQ' || CASE
        WHEN MONTH(d.date_value) IN (7,8,9) THEN 1
        WHEN MONTH(d.date_value) IN (10,11,12) THEN 2
        WHEN MONTH(d.date_value) IN (1,2,3) THEN 3
        ELSE 4
    END AS fiscal_quarter_name,

    -- Flags
    CASE WHEN DAYOFWEEK(d.date_value) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    NULL AS holiday_name,
    CASE WHEN DAYOFWEEK(d.date_value) NOT IN (0, 6) THEN TRUE ELSE FALSE END AS is_business_day,

    -- Relative flags (updated dynamically via view or task)
    CASE WHEN d.date_value = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_current_day,
    CASE WHEN WEEKOFYEAR(d.date_value) = WEEKOFYEAR(CURRENT_DATE()) AND YEAR(d.date_value) = YEAR(CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_week,
    CASE WHEN MONTH(d.date_value) = MONTH(CURRENT_DATE()) AND YEAR(d.date_value) = YEAR(CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_month,
    CASE WHEN QUARTER(d.date_value) = QUARTER(CURRENT_DATE()) AND YEAR(d.date_value) = YEAR(CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_quarter,
    CASE WHEN YEAR(d.date_value) = YEAR(CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_year
FROM (
    SELECT DATEADD('day', SEQ4(), '2020-01-01')::DATE AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 2192))  -- ~6 years
) d;

-- =============================================================================
-- SECTION 3: UPDATE US HOLIDAYS
-- =============================================================================

-- Update major US holidays (simplified - extend as needed)
UPDATE DIM_DATE SET is_holiday = TRUE, holiday_name = 'New Year''s Day', is_business_day = FALSE
WHERE month_number = 1 AND day_of_month = 1;

UPDATE DIM_DATE SET is_holiday = TRUE, holiday_name = 'Independence Day', is_business_day = FALSE
WHERE month_number = 7 AND day_of_month = 4;

UPDATE DIM_DATE SET is_holiday = TRUE, holiday_name = 'Christmas Day', is_business_day = FALSE
WHERE month_number = 12 AND day_of_month = 25;

UPDATE DIM_DATE SET is_holiday = TRUE, holiday_name = 'Thanksgiving', is_business_day = FALSE
WHERE month_number = 11 AND day_name = 'Thu' AND day_of_month BETWEEN 22 AND 28;

UPDATE DIM_DATE SET is_holiday = TRUE, holiday_name = 'Memorial Day', is_business_day = FALSE
WHERE month_number = 5 AND day_name = 'Mon' AND day_of_month >= 25;

UPDATE DIM_DATE SET is_holiday = TRUE, holiday_name = 'Labor Day', is_business_day = FALSE
WHERE month_number = 9 AND day_name = 'Mon' AND day_of_month <= 7;

-- =============================================================================
-- SECTION 4: HELPER VIEWS
-- =============================================================================

-- Current month calendar
CREATE OR REPLACE VIEW VW_CURRENT_MONTH_CALENDAR AS
SELECT date_key, full_date, day_name, is_weekend, is_holiday, is_business_day
FROM DIM_DATE
WHERE is_current_month = TRUE
ORDER BY full_date;

-- Business days count per month
CREATE OR REPLACE VIEW VW_BUSINESS_DAYS_BY_MONTH AS
SELECT
    year_number,
    month_number,
    month_name,
    COUNT(*) AS total_days,
    SUM(CASE WHEN is_business_day THEN 1 ELSE 0 END) AS business_days,
    SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_days,
    SUM(CASE WHEN is_holiday THEN 1 ELSE 0 END) AS holidays
FROM DIM_DATE
GROUP BY year_number, month_number, month_name
ORDER BY year_number, month_number;

-- =============================================================================
-- SECTION 5: VERIFY AND GRANT
-- =============================================================================

-- Verify population
SELECT
    MIN(full_date) AS earliest_date,
    MAX(full_date) AS latest_date,
    COUNT(*) AS total_days,
    COUNT(DISTINCT year_number) AS years_covered,
    SUM(CASE WHEN is_holiday THEN 1 ELSE 0 END) AS total_holidays
FROM DIM_DATE;

GRANT SELECT ON TABLE DIM_DATE TO ROLE RETAIL_ANALYST;
GRANT SELECT ON TABLE DIM_DATE TO ROLE RETAIL_VIEWER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DIM_DATE TO ROLE RETAIL_ENGINEER;
GRANT ALL PRIVILEGES ON TABLE DIM_DATE TO ROLE RETAIL_ADMIN;

-- =============================================================================
-- INTERVIEW Q&A
-- =============================================================================
/*
Q1: Why use an integer date_key instead of a DATE column?
A1: Integer keys (YYYYMMDD format):
    - Faster joins than DATE comparison
    - Human-readable (20240115 = Jan 15, 2024)
    - Consistent with data warehouse best practices
    - Efficient for partition pruning

Q2: Why pre-populate the date dimension?
A2: - Ensures all dates exist even if no transactions occur
    - Enables calendar-based analysis (business days, holidays)
    - Avoids gaps in time-series reports
    - Can be populated once and extended annually

Q3: How do you handle fiscal vs calendar year?
A3: Include both in the dimension:
    - Calendar: standard year/quarter/month
    - Fiscal: based on company's fiscal calendar
    - Allows analysts to report on either without complex logic
    - Common fiscal year starts: January (calendar), July, October
*/
