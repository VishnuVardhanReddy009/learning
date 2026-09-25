-- ============================================================
-- FILE: 03_silver/01_silver_tables.sql
-- PURPOSE: Silver layer tables — typed, validated, SCD2 dims
-- LAYER: SILVER (Cleansed)
-- RUN AS: DATA_ENGINEER_ROLE
-- ============================================================
-- Silver applies: type casting, null handling, deduplication,
-- business key validation, SCD2 for dimensions.
-- All columns are properly typed (no more raw VARCHAR).
-- ============================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE SILVER_DB;

-- ─────────────────────────────────────────────────────────────
-- DIM_CUSTOMERS — SCD Type 2
-- Tracks full history of customer attribute changes
-- ─────────────────────────────────────────────────────────────
USE SCHEMA DIMENSIONS;

CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS (
    -- Surrogate key (generated, never from source)
    SK_CUSTOMER         NUMBER DEFAULT COMMON_DB.UTILITIES.SEQ_CUSTOMER_SK.NEXTVAL,
    -- Natural/business key
    CUSTOMER_ID         VARCHAR(50)  NOT NULL,
    -- Attributes
    CUSTOMER_NAME       VARCHAR(200),
    EMAIL               VARCHAR(200),
    PHONE               VARCHAR(30),
    SEGMENT             VARCHAR(30),  -- Consumer | Corporate | Home Office
    CITY                VARCHAR(100),
    STATE               VARCHAR(100),
    COUNTRY             VARCHAR(100),
    POSTAL_CODE         VARCHAR(20),
    REGION_ID           VARCHAR(20),
    REGISTRATION_DATE   DATE,
    CREDIT_LIMIT        NUMBER(12,2),
    -- SCD2 tracking columns
    EFF_START_DATE      DATE         NOT NULL,
    EFF_END_DATE        DATE,                   -- NULL = current record
    IS_CURRENT          BOOLEAN      DEFAULT TRUE,
    -- DW metadata
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    CONSTRAINT PK_DIM_CUSTOMERS PRIMARY KEY (SK_CUSTOMER)
)
CLUSTER BY (CUSTOMER_ID, IS_CURRENT)
COMMENT = 'SCD Type 2 customer dimension — full attribute history';

-- ─────────────────────────────────────────────────────────────
-- DIM_PRODUCTS — SCD Type 2
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_PRODUCTS (
    SK_PRODUCT          NUMBER DEFAULT COMMON_DB.UTILITIES.SEQ_PRODUCT_SK.NEXTVAL,
    PRODUCT_ID          VARCHAR(50)  NOT NULL,
    PRODUCT_NAME        VARCHAR(300),
    CATEGORY            VARCHAR(100),
    SUB_CATEGORY        VARCHAR(100),
    BRAND               VARCHAR(100),
    UNIT_COST           NUMBER(10,2),
    UNIT_PRICE          NUMBER(10,2),
    SUPPLIER_ID         VARCHAR(50),
    IS_ACTIVE           BOOLEAN,
    LAUNCH_DATE         DATE,
    -- Derived
    MARGIN_PCT          NUMBER(6,3)  AS (
                            CASE WHEN UNIT_PRICE > 0
                                 THEN ((UNIT_PRICE - UNIT_COST) / UNIT_PRICE) * 100
                                 ELSE NULL END
                        ),
    PRICE_TIER          VARCHAR(20)  AS (
                            CASE
                                WHEN UNIT_PRICE <  25  THEN 'Budget'
                                WHEN UNIT_PRICE <  100 THEN 'Mid-Range'
                                WHEN UNIT_PRICE <  500 THEN 'Premium'
                                ELSE 'Luxury'
                            END
                        ),
    -- SCD2
    EFF_START_DATE      DATE         NOT NULL,
    EFF_END_DATE        DATE,
    IS_CURRENT          BOOLEAN      DEFAULT TRUE,
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    CONSTRAINT PK_DIM_PRODUCTS PRIMARY KEY (SK_PRODUCT)
)
CLUSTER BY (PRODUCT_ID, IS_CURRENT)
COMMENT = 'SCD Type 2 product dimension — tracks price and attribute changes';

-- ─────────────────────────────────────────────────────────────
-- DIM_REGIONS — SCD Type 1 (overwrite — no history needed)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_REGIONS (
    SK_REGION           NUMBER DEFAULT COMMON_DB.UTILITIES.SEQ_REGION_SK.NEXTVAL,
    REGION_ID           VARCHAR(20)  NOT NULL,
    REGION_NAME         VARCHAR(100),
    COUNTRY             VARCHAR(100),
    COUNTRY_CODE        VARCHAR(10),
    TIMEZONE            VARCHAR(50),
    SALES_TERRITORY     VARCHAR(100),
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_REGIONS PRIMARY KEY (SK_REGION),
    CONSTRAINT UQ_REGION_ID   UNIQUE (REGION_ID)  -- informational
)
COMMENT = 'Region reference dimension — SCD Type 1 (overwrite)';

-- ─────────────────────────────────────────────────────────────
-- DIM_DATE — Pre-built date dimension (2018-2030)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_KEY            NUMBER        NOT NULL,  -- YYYYMMDD integer
    FULL_DATE           DATE          NOT NULL,
    DAY_OF_WEEK         NUMBER(1),               -- 1=Mon, 7=Sun
    DAY_NAME            VARCHAR(10),
    DAY_OF_MONTH        NUMBER(2),
    DAY_OF_YEAR         NUMBER(3),
    WEEK_OF_YEAR        NUMBER(2),
    MONTH_NUM           NUMBER(2),
    MONTH_NAME          VARCHAR(10),
    MONTH_SHORT         VARCHAR(3),
    QUARTER             NUMBER(1),
    QUARTER_NAME        VARCHAR(6),
    YEAR                NUMBER(4),
    IS_WEEKEND          BOOLEAN,
    IS_HOLIDAY          BOOLEAN       DEFAULT FALSE,
    FISCAL_YEAR         NUMBER(4),
    FISCAL_QUARTER      NUMBER(1),
    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_KEY)
)
COMMENT = 'Date dimension populated for 2018-2030';

-- Populate date dimension
INSERT INTO DIM_DATE
WITH DATE_SPINE AS (
    SELECT DATEADD('day', SEQ4(), '2018-01-01'::DATE) AS FULL_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 4748))  -- 2018 to 2030
)
SELECT
    TO_NUMBER(TO_CHAR(FULL_DATE, 'YYYYMMDD'))            AS DATE_KEY,
    FULL_DATE,
    DAYOFWEEKISO(FULL_DATE)                              AS DAY_OF_WEEK,
    DAYNAME(FULL_DATE)                                   AS DAY_NAME,
    DAYOFMONTH(FULL_DATE)                                AS DAY_OF_MONTH,
    DAYOFYEAR(FULL_DATE)                                 AS DAY_OF_YEAR,
    WEEKOFYEAR(FULL_DATE)                                AS WEEK_OF_YEAR,
    MONTH(FULL_DATE)                                     AS MONTH_NUM,
    MONTHNAME(FULL_DATE)                                 AS MONTH_NAME,
    LEFT(MONTHNAME(FULL_DATE), 3)                        AS MONTH_SHORT,
    QUARTER(FULL_DATE)                                   AS QUARTER,
    'Q' || QUARTER(FULL_DATE)                            AS QUARTER_NAME,
    YEAR(FULL_DATE)                                      AS YEAR,
    DAYOFWEEKISO(FULL_DATE) IN (6, 7)                    AS IS_WEEKEND,
    FALSE                                                AS IS_HOLIDAY,
    CASE WHEN MONTH(FULL_DATE) >= 4
         THEN YEAR(FULL_DATE)
         ELSE YEAR(FULL_DATE) - 1 END                    AS FISCAL_YEAR,
    CASE
        WHEN MONTH(FULL_DATE) IN (4,5,6)   THEN 1
        WHEN MONTH(FULL_DATE) IN (7,8,9)   THEN 2
        WHEN MONTH(FULL_DATE) IN (10,11,12) THEN 3
        ELSE 4
    END                                                  AS FISCAL_QUARTER
FROM DATE_SPINE;

-- ─────────────────────────────────────────────────────────────
-- FACT_ORDERS — Grain: one row per order
-- ─────────────────────────────────────────────────────────────
USE SCHEMA FACTS;

CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    ORDER_SK            NUMBER AUTOINCREMENT PRIMARY KEY,
    -- Natural keys (kept for lineage)
    ORDER_ID            VARCHAR(50)  NOT NULL,
    -- Foreign keys to dimensions (surrogate keys)
    SK_CUSTOMER         NUMBER,
    SK_REGION           NUMBER,
    -- Date keys (joins to DIM_DATE)
    ORDER_DATE_KEY      NUMBER,
    SHIP_DATE_KEY       NUMBER,
    -- Measures
    TOTAL_AMOUNT        NUMBER(12,2),
    DISCOUNT_PCT        NUMBER(6,3),
    DISCOUNT_AMOUNT     NUMBER(12,2)  AS (TOTAL_AMOUNT * DISCOUNT_PCT / 100),
    NET_AMOUNT          NUMBER(12,2)  AS (TOTAL_AMOUNT - (TOTAL_AMOUNT * DISCOUNT_PCT / 100)),
    PROFIT              NUMBER(12,2),
    -- Descriptive
    STATUS              VARCHAR(20),
    SHIP_MODE           VARCHAR(30),
    DAYS_TO_SHIP        NUMBER        AS (DATEDIFF('day',
                            TO_DATE(ORDER_DATE_KEY::VARCHAR, 'YYYYMMDD'),
                            TO_DATE(SHIP_DATE_KEY::VARCHAR, 'YYYYMMDD')
                        )),
    -- DW metadata
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    CONSTRAINT UQ_FACT_ORDER_ID UNIQUE (ORDER_ID)
)
CLUSTER BY (ORDER_DATE_KEY, SK_CUSTOMER)
COMMENT = 'Fact table — grain: one row per order header';

-- ─────────────────────────────────────────────────────────────
-- FACT_ORDER_ITEMS — Grain: one row per order line item
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FACT_ORDER_ITEMS (
    ITEM_SK             NUMBER AUTOINCREMENT PRIMARY KEY,
    ITEM_ID             VARCHAR(50)  NOT NULL,
    ORDER_ID            VARCHAR(50),
    -- Foreign keys
    SK_CUSTOMER         NUMBER,
    SK_PRODUCT          NUMBER,
    SK_REGION           NUMBER,
    ORDER_DATE_KEY      NUMBER,
    -- Measures
    QUANTITY            NUMBER,
    UNIT_PRICE          NUMBER(10,2),
    DISCOUNT_AMOUNT     NUMBER(10,2),
    LINE_TOTAL          NUMBER(12,2),
    RETURN_FLAG         BOOLEAN,
    -- DW metadata
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    CONSTRAINT UQ_ITEM_ID UNIQUE (ITEM_ID)
)
CLUSTER BY (ORDER_DATE_KEY, SK_PRODUCT, SK_CUSTOMER)
COMMENT = 'Fact table — grain: one row per order line item';

SHOW TABLES IN SCHEMA SILVER_DB.DIMENSIONS;
SHOW TABLES IN SCHEMA SILVER_DB.FACTS;
