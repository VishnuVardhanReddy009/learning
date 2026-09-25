-- ============================================================
-- FILE: 02_bronze/02_copy_into_snowpipe.sql
-- PURPOSE: COPY INTO for batch loads + Snowpipe for continuous
-- LAYER: BRONZE (Raw Landing)
-- RUN AS: ETL_ROLE
-- ============================================================

USE ROLE ETL_ROLE;
USE WAREHOUSE INGESTION_WH;
USE DATABASE BRONZE_DB;
USE SCHEMA RAW;

-- ─────────────────────────────────────────────────────────────
-- SECTION A: MANUAL / SCHEDULED BATCH COPY INTO
-- Use this for: initial historical loads, backfills, reruns
-- ─────────────────────────────────────────────────────────────

-- ── INFER SCHEMA (run once to understand file structure) ──────
-- Validate file structure before committing to a load
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION    => '@COMMON_DB.INGESTION.STG_S3_ORDERS',
        FILE_FORMAT => 'COMMON_DB.INGESTION.CSV_STANDARD_FMT'
    )
);

-- Preview raw data from stage (no warehouse needed)
SELECT $1, $2, $3, $4, $5
FROM @COMMON_DB.INGESTION.STG_S3_ORDERS
(FILE_FORMAT => 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
LIMIT 10;

-- ── VALIDATE before loading (dry run, no data inserted) ───────
COPY INTO BRONZE_DB.RAW.RAW_ORDERS (
    ORDER_ID, CUSTOMER_ID, ORDER_DATE, SHIP_DATE, STATUS,
    SHIP_MODE, REGION_ID, TOTAL_AMOUNT, DISCOUNT_PCT, PROFIT,
    _LOAD_FILE
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           METADATA$FILENAME
    FROM @COMMON_DB.INGESTION.STG_S3_ORDERS
)
FILE_FORMAT     = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
PATTERN         = '.*orders_.*\\.csv(\\.gz)?'
VALIDATION_MODE = 'RETURN_ALL_ERRORS';   -- show all errors, load nothing

-- ── LOAD ORDERS — with full error handling ────────────────────
COPY INTO BRONZE_DB.RAW.RAW_ORDERS (
    ORDER_ID, CUSTOMER_ID, ORDER_DATE, SHIP_DATE, STATUS,
    SHIP_MODE, REGION_ID, TOTAL_AMOUNT, DISCOUNT_PCT, PROFIT,
    _LOAD_FILE
)
FROM (
    SELECT
        $1,                       -- ORDER_ID
        $2,                       -- CUSTOMER_ID
        $3,                       -- ORDER_DATE
        $4,                       -- SHIP_DATE
        $5,                       -- STATUS
        $6,                       -- SHIP_MODE
        $7,                       -- REGION_ID
        $8,                       -- TOTAL_AMOUNT
        $9,                       -- DISCOUNT_PCT
        $10,                      -- PROFIT
        METADATA$FILENAME         -- capture source filename
    FROM @COMMON_DB.INGESTION.STG_S3_ORDERS
)
FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
PATTERN     = '.*orders_.*\\.csv(\\.gz)?'
ON_ERROR    = 'CONTINUE'          -- skip bad rows, continue loading
PURGE       = FALSE               -- keep files in S3 (for reprocessing)
FORCE       = FALSE;              -- respect 64-day load history (prevent dups)

-- ── LOAD CUSTOMERS ────────────────────────────────────────────
COPY INTO BRONZE_DB.RAW.RAW_CUSTOMERS (
    CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, SEGMENT,
    CITY, STATE, COUNTRY, POSTAL_CODE, REGION_ID,
    REGISTRATION_DATE, CREDIT_LIMIT, _LOAD_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
           METADATA$FILENAME
    FROM @COMMON_DB.INGESTION.STG_S3_CUSTOMERS
)
FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
PATTERN     = '.*customers_.*\\.csv(\\.gz)?'
ON_ERROR    = 'CONTINUE';

-- ── LOAD PRODUCTS ─────────────────────────────────────────────
COPY INTO BRONZE_DB.RAW.RAW_PRODUCTS (
    PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY,
    BRAND, UNIT_COST, UNIT_PRICE, SUPPLIER_ID,
    IS_ACTIVE, LAUNCH_DATE, _LOAD_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
           METADATA$FILENAME
    FROM @COMMON_DB.INGESTION.STG_S3_PRODUCTS
)
FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
PATTERN     = '.*products_.*\\.csv(\\.gz)?'
ON_ERROR    = 'CONTINUE';

-- ── LOAD ORDER ITEMS ──────────────────────────────────────────
COPY INTO BRONZE_DB.RAW.RAW_ORDER_ITEMS (
    ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY,
    UNIT_PRICE, DISCOUNT_AMOUNT, LINE_TOTAL, RETURN_FLAG, _LOAD_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,
           METADATA$FILENAME
    FROM @COMMON_DB.INGESTION.STG_S3_ORDER_ITEMS
)
FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
PATTERN     = '.*order_items_.*\\.csv(\\.gz)?'
ON_ERROR    = 'CONTINUE';

-- ── LOAD REGIONS ──────────────────────────────────────────────
COPY INTO BRONZE_DB.RAW.RAW_REGIONS (
    REGION_ID, REGION_NAME, COUNTRY, COUNTRY_CODE,
    TIMEZONE, SALES_TERRITORY, _LOAD_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,
           METADATA$FILENAME
    FROM @COMMON_DB.INGESTION.STG_S3_REGIONS
)
FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
ON_ERROR    = 'CONTINUE';

-- ─────────────────────────────────────────────────────────────
-- SECTION B: SNOWPIPE — Continuous auto-ingestion
-- Triggers automatically when new files land in S3
-- ─────────────────────────────────────────────────────────────

USE ROLE DATA_ENGINEER_ROLE;
USE DATABASE COMMON_DB;
USE SCHEMA INGESTION;

-- Pipe: Orders (most frequent — daily files)
CREATE PIPE IF NOT EXISTS PIPE_ORDERS
    AUTO_INGEST = TRUE
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT = 'Auto-ingest orders CSV from S3 — triggers on S3 ObjectCreated event'
AS
    COPY INTO BRONZE_DB.RAW.RAW_ORDERS (
        ORDER_ID, CUSTOMER_ID, ORDER_DATE, SHIP_DATE, STATUS,
        SHIP_MODE, REGION_ID, TOTAL_AMOUNT, DISCOUNT_PCT, PROFIT,
        _LOAD_FILE
    )
    FROM (
        SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
               METADATA$FILENAME
        FROM @COMMON_DB.INGESTION.STG_S3_ORDERS
    )
    FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    ON_ERROR    = 'CONTINUE';

-- Pipe: Customers
CREATE PIPE IF NOT EXISTS PIPE_CUSTOMERS
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest customers CSV from S3'
AS
    COPY INTO BRONZE_DB.RAW.RAW_CUSTOMERS (
        CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, SEGMENT,
        CITY, STATE, COUNTRY, POSTAL_CODE, REGION_ID,
        REGISTRATION_DATE, CREDIT_LIMIT, _LOAD_FILE
    )
    FROM (
        SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
               METADATA$FILENAME
        FROM @COMMON_DB.INGESTION.STG_S3_CUSTOMERS
    )
    FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    ON_ERROR    = 'CONTINUE';

-- Pipe: Order Items
CREATE PIPE IF NOT EXISTS PIPE_ORDER_ITEMS
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest order items CSV from S3'
AS
    COPY INTO BRONZE_DB.RAW.RAW_ORDER_ITEMS (
        ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY,
        UNIT_PRICE, DISCOUNT_AMOUNT, LINE_TOTAL, RETURN_FLAG, _LOAD_FILE
    )
    FROM (
        SELECT $1,$2,$3,$4,$5,$6,$7,$8,
               METADATA$FILENAME
        FROM @COMMON_DB.INGESTION.STG_S3_ORDER_ITEMS
    )
    FILE_FORMAT = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    ON_ERROR    = 'CONTINUE';

-- ── GET SQS ARNs for AWS S3 Event Notification setup ──────────
SHOW PIPES;
-- For each pipe, copy notification_channel (SQS ARN)
-- In AWS S3 console: bucket → Properties → Event Notifications
-- Add: ObjectCreated → SQS → paste the ARN

-- Monitor pipe status
SELECT SYSTEM$PIPE_STATUS('COMMON_DB.INGESTION.PIPE_ORDERS');
SELECT SYSTEM$PIPE_STATUS('COMMON_DB.INGESTION.PIPE_CUSTOMERS');

-- Check load history
SELECT FILE_NAME, ROW_COUNT, ROW_PARSED, ERROR_COUNT, STATUS, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_ORDERS',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- Manually refresh pipe (catch up missed files after outage)
ALTER PIPE COMMON_DB.INGESTION.PIPE_ORDERS REFRESH
    MODIFIED_AFTER = DATEADD('hour', -6, CURRENT_TIMESTAMP());
