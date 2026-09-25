-- ============================================================
-- FILE: 02_bronze/01_bronze_tables.sql
-- PURPOSE: Create raw Bronze layer tables — exact source shape
-- LAYER: BRONZE (Raw Landing)
-- RUN AS: DATA_ENGINEER_ROLE
-- ============================================================
-- Bronze tables are TRANSIENT — no Fail-safe, lower storage cost.
-- They mirror the CSV source exactly. No business logic applied.
-- All columns are VARCHAR to handle bad data gracefully.
-- ============================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE INGESTION_WH;
USE DATABASE BRONZE_DB;
USE SCHEMA RAW;

-- ─────────────────────────────────────────────────────────────
-- ORDERS (transactional fact)
-- S3 Path: s3://retail-analytics-bucket/raw/orders/
-- File: orders_YYYYMMDD.csv
-- Frequency: Daily batch
-- ─────────────────────────────────────────────────────────────
CREATE TRANSIENT TABLE IF NOT EXISTS RAW_ORDERS (
    -- Source columns (all VARCHAR — raw, unvalidated)
    ORDER_ID            VARCHAR(50),
    CUSTOMER_ID         VARCHAR(50),
    ORDER_DATE          VARCHAR(20),
    SHIP_DATE           VARCHAR(20),
    STATUS              VARCHAR(20),
    SHIP_MODE           VARCHAR(30),
    REGION_ID           VARCHAR(20),
    TOTAL_AMOUNT        VARCHAR(20),
    DISCOUNT_PCT        VARCHAR(10),
    PROFIT              VARCHAR(20),
    -- ETL metadata (added by COPY INTO)
    _LOAD_FILE          VARCHAR(500),
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _LOAD_ROW_NUMBER    NUMBER AUTOINCREMENT,
    _SOURCE_SYSTEM      VARCHAR(50)   DEFAULT 'S3_RETAIL',
    _IS_PROCESSED       BOOLEAN       DEFAULT FALSE
)
COMMENT = 'Raw orders from S3 — transient, no transforms applied';

-- ─────────────────────────────────────────────────────────────
-- CUSTOMERS (dimension source)
-- ─────────────────────────────────────────────────────────────
CREATE TRANSIENT TABLE IF NOT EXISTS RAW_CUSTOMERS (
    CUSTOMER_ID         VARCHAR(50),
    CUSTOMER_NAME       VARCHAR(200),
    EMAIL               VARCHAR(200),
    PHONE               VARCHAR(30),
    SEGMENT             VARCHAR(30),
    CITY                VARCHAR(100),
    STATE               VARCHAR(100),
    COUNTRY             VARCHAR(100),
    POSTAL_CODE         VARCHAR(20),
    REGION_ID           VARCHAR(20),
    REGISTRATION_DATE   VARCHAR(20),
    CREDIT_LIMIT        VARCHAR(20),
    -- ETL metadata
    _LOAD_FILE          VARCHAR(500),
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _LOAD_ROW_NUMBER    NUMBER AUTOINCREMENT,
    _SOURCE_SYSTEM      VARCHAR(50)   DEFAULT 'S3_RETAIL',
    _IS_PROCESSED       BOOLEAN       DEFAULT FALSE
)
COMMENT = 'Raw customer master from S3 — transient';

-- ─────────────────────────────────────────────────────────────
-- PRODUCTS (dimension source)
-- ─────────────────────────────────────────────────────────────
CREATE TRANSIENT TABLE IF NOT EXISTS RAW_PRODUCTS (
    PRODUCT_ID          VARCHAR(50),
    PRODUCT_NAME        VARCHAR(300),
    CATEGORY            VARCHAR(100),
    SUB_CATEGORY        VARCHAR(100),
    BRAND               VARCHAR(100),
    UNIT_COST           VARCHAR(20),
    UNIT_PRICE          VARCHAR(20),
    SUPPLIER_ID         VARCHAR(50),
    IS_ACTIVE           VARCHAR(5),
    LAUNCH_DATE         VARCHAR(20),
    -- ETL metadata
    _LOAD_FILE          VARCHAR(500),
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _LOAD_ROW_NUMBER    NUMBER AUTOINCREMENT,
    _SOURCE_SYSTEM      VARCHAR(50)   DEFAULT 'S3_RETAIL',
    _IS_PROCESSED       BOOLEAN       DEFAULT FALSE
)
COMMENT = 'Raw product catalog from S3 — transient';

-- ─────────────────────────────────────────────────────────────
-- ORDER ITEMS (fact line items)
-- ─────────────────────────────────────────────────────────────
CREATE TRANSIENT TABLE IF NOT EXISTS RAW_ORDER_ITEMS (
    ITEM_ID             VARCHAR(50),
    ORDER_ID            VARCHAR(50),
    PRODUCT_ID          VARCHAR(50),
    QUANTITY            VARCHAR(10),
    UNIT_PRICE          VARCHAR(20),
    DISCOUNT_AMOUNT     VARCHAR(20),
    LINE_TOTAL          VARCHAR(20),
    RETURN_FLAG         VARCHAR(5),
    -- ETL metadata
    _LOAD_FILE          VARCHAR(500),
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _LOAD_ROW_NUMBER    NUMBER AUTOINCREMENT,
    _SOURCE_SYSTEM      VARCHAR(50)   DEFAULT 'S3_RETAIL',
    _IS_PROCESSED       BOOLEAN       DEFAULT FALSE
)
COMMENT = 'Raw order line items from S3 — transient';

-- ─────────────────────────────────────────────────────────────
-- REGIONS (reference / slowly changing)
-- ─────────────────────────────────────────────────────────────
CREATE TRANSIENT TABLE IF NOT EXISTS RAW_REGIONS (
    REGION_ID           VARCHAR(20),
    REGION_NAME         VARCHAR(100),
    COUNTRY             VARCHAR(100),
    COUNTRY_CODE        VARCHAR(10),
    TIMEZONE            VARCHAR(50),
    SALES_TERRITORY     VARCHAR(100),
    -- ETL metadata
    _LOAD_FILE          VARCHAR(500),
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _LOAD_ROW_NUMBER    NUMBER AUTOINCREMENT,
    _SOURCE_SYSTEM      VARCHAR(50)   DEFAULT 'S3_RETAIL',
    _IS_PROCESSED       BOOLEAN       DEFAULT FALSE
)
COMMENT = 'Raw region reference data from S3 — transient';

-- ─────────────────────────────────────────────────────────────
-- AUDIT LOG (tracks every load operation)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS BRONZE_DB.ARCHIVE.LOAD_AUDIT_LOG (
    LOG_ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    LOAD_TIMESTAMP      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TABLE_NAME          VARCHAR(100),
    STAGE_NAME          VARCHAR(200),
    FILE_NAME           VARCHAR(500),
    ROWS_LOADED         NUMBER        DEFAULT 0,
    ROWS_REJECTED       NUMBER        DEFAULT 0,
    STATUS              VARCHAR(20),  -- SUCCESS | PARTIAL | FAILED
    ERROR_MESSAGE       VARCHAR(2000),
    LOAD_DURATION_SEC   NUMBER,
    LOADED_BY           VARCHAR(100)  DEFAULT CURRENT_USER()
)
COMMENT = 'Audit log for all Bronze layer loads';

SHOW TABLES IN SCHEMA BRONZE_DB.RAW;
