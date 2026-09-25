-- ============================================================
-- FILE: 03_cdc_engine/01_cdc_streams_control.sql
-- PURPOSE: Complete CDC engine — streams on Bronze tables,
--          change classification, control tables, dedup logic
-- CONCEPT: CDC = Change Data Capture — detect INSERT/UPDATE/DELETE
--          from source and propagate only changed rows downstream
-- ============================================================

USE ROLE DATA_ENG_ROLE;
USE WAREHOUSE TRANSFORM_WH;

-- ═══════════════════════════════════════════════════════════════
-- SECTION 1: CDC THEORY & SNOWFLAKE IMPLEMENTATION
-- ═══════════════════════════════════════════════════════════════
--
-- CDC PATTERNS USED IN THIS PROJECT:
--
-- Pattern 1: HASH-BASED CDC (Bronze → CDC_DB)
--   - Compute MD5 of all business columns at load time (_SRC_ROW_HASH)
--   - Compare current hash vs last-seen hash in CDC_CONTROL table
--   - If hash changed → row is a change candidate
--   - Handles: full-file source extracts with no updated_at column
--
-- Pattern 2: STREAM-BASED CDC (Bronze → Silver via Snowflake Streams)
--   - Snowflake Streams track every DML on Bronze tables
--   - METADATA$ACTION = INSERT or DELETE (updates appear as DELETE+INSERT)
--   - METADATA$ISUPDATE = TRUE when it's a paired UPDATE
--   - Offset advances ONLY on successful DML commit → exactly-once
--
-- Pattern 3: TIMESTAMP-BASED CDC (as fallback)
--   - Use _LOAD_TS watermark to process only rows loaded since last run
--   - Simpler but misses deletes — combined with hash for completeness
--
-- Pattern 4: LOG-BASED CDC (conceptual — via Debezium/Kafka)
--   - Real-time row-level changes from DB transaction log
--   - Arrives in Snowflake via Kafka Connector as JSON events
--   - Shown in Section 7 of this file
--
-- ═══════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────
-- SECTION 2: CDC CONTROL TABLE
-- Tracks the last-seen hash per business key per table
-- Used for HASH-BASED change detection
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS CDC_DB.CONTROL.CDC_STATE (
    STATE_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    TABLE_NAME      VARCHAR(100)  NOT NULL,     -- e.g. 'RAW_CUSTOMERS'
    BUSINESS_KEY    VARCHAR(200)  NOT NULL,     -- e.g. customer_id value
    LAST_HASH       VARCHAR(64),               -- MD5 of last processed row
    LAST_DW_SK      NUMBER,                    -- surrogate key in Silver
    CURRENT_VERSION NUMBER DEFAULT 1,          -- SCD2 version counter
    FIRST_SEEN_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_SEEN_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED      BOOLEAN DEFAULT FALSE,     -- soft-delete tracking
    CONSTRAINT UQ_CDC_STATE UNIQUE (TABLE_NAME, BUSINESS_KEY)
);

-- Watermark table: tracks last successful run per pipeline step
CREATE TABLE IF NOT EXISTS CDC_DB.CONTROL.PIPELINE_WATERMARK (
    WATERMARK_ID    NUMBER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME   VARCHAR(100) NOT NULL,
    STEP_NAME       VARCHAR(100) NOT NULL,
    LAST_RUN_AT     TIMESTAMP_NTZ,
    LAST_BATCH_ID   VARCHAR(100),
    ROWS_PROCESSED  NUMBER DEFAULT 0,
    STATUS          VARCHAR(20),   -- SUCCESS|FAILED|RUNNING
    UPDATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT UQ_WATERMARK UNIQUE (PIPELINE_NAME, STEP_NAME)
);

-- Change event log: full audit trail of every CDC event
CREATE TABLE IF NOT EXISTS CDC_DB.CONTROL.CDC_EVENT_LOG (
    EVENT_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    EVENT_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID        VARCHAR(100),
    TABLE_NAME      VARCHAR(100),
    OPERATION       VARCHAR(10),   -- INSERT|UPDATE|DELETE|UPSERT
    BUSINESS_KEY    VARCHAR(200),
    BEFORE_HASH     VARCHAR(64),   -- hash before change
    AFTER_HASH      VARCHAR(64),   -- hash after change
    CHANGED_COLUMNS VARIANT,       -- JSON array of changed column names
    DW_TABLE_NAME   VARCHAR(200),  -- target DW table
    DW_SK           NUMBER,        -- surrogate key in target
    SCD_ACTION      VARCHAR(20),   -- NEW|EXPIRE|INSERT_CURRENT|NO_CHANGE
    NOTES           VARCHAR(500)
);

-- ─────────────────────────────────────────────────────────────
-- SECTION 3: SNOWFLAKE STREAMS on Bronze tables
-- These are the core CDC mechanism for Silver loading
-- ─────────────────────────────────────────────────────────────

-- CUSTOMERS stream — STANDARD (tracks INSERT + UPDATE + DELETE)
-- We use STANDARD because customer master data can be updated
CREATE STREAM IF NOT EXISTS CDC_DB.STREAMS.STR_CUSTOMERS
    ON TABLE BRONZE_DB.RAW.RAW_CUSTOMERS
    COMMENT = 'Standard CDC stream on raw_customers — captures all DML';
-- METADATA$ACTION    : INSERT or DELETE
-- METADATA$ISUPDATE  : TRUE when DELETE+INSERT pair represents an UPDATE
-- METADATA$ROW_ID    : Snowflake internal unique row identifier

-- PRODUCTS stream — STANDARD (price and category changes)
CREATE STREAM IF NOT EXISTS CDC_DB.STREAMS.STR_PRODUCTS
    ON TABLE BRONZE_DB.RAW.RAW_PRODUCTS
    COMMENT = 'Standard CDC stream on raw_products — tracks price changes';

-- ORDERS stream — APPEND_ONLY (orders only insert, status changes tracked separately)
CREATE STREAM IF NOT EXISTS CDC_DB.STREAMS.STR_ORDERS
    ON TABLE BRONZE_DB.RAW.RAW_ORDERS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only stream on raw_orders — insert-only source';

-- ORDER ITEMS stream — APPEND_ONLY
CREATE STREAM IF NOT EXISTS CDC_DB.STREAMS.STR_ORDER_ITEMS
    ON TABLE BRONZE_DB.RAW.RAW_ORDER_ITEMS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only stream for order line items';

-- EMPLOYEES stream — STANDARD (department/title/salary changes = SCD2)
CREATE STREAM IF NOT EXISTS CDC_DB.STREAMS.STR_EMPLOYEES
    ON TABLE BRONZE_DB.RAW.RAW_EMPLOYEES
    COMMENT = 'Standard CDC stream on raw_employees — org changes';

-- STORES stream — STANDARD (SCD Type 1 — overwrite)
CREATE STREAM IF NOT EXISTS CDC_DB.STREAMS.STR_STORES
    ON TABLE BRONZE_DB.RAW.RAW_STORES
    COMMENT = 'Standard CDC stream on raw_stores — SCD1 overwrite';

-- ─────────────────────────────────────────────────────────────
-- SECTION 4: CDC STAGING TABLES
-- Intermediate area where change classification happens
-- before final write to Silver dimensions
-- ─────────────────────────────────────────────────────────────

-- Customer CDC staging
CREATE OR REPLACE TRANSIENT TABLE CDC_DB.STAGING.STG_CUSTOMER_CHANGES (
    STAGE_ID            NUMBER AUTOINCREMENT,
    BATCH_ID            VARCHAR(100),
    CUSTOMER_ID         VARCHAR(50)  NOT NULL,
    -- All business columns
    CUSTOMER_NAME       VARCHAR(200),
    EMAIL               VARCHAR(200),
    PHONE               VARCHAR(30),
    SEGMENT             VARCHAR(30),
    CITY                VARCHAR(100),
    STATE               VARCHAR(100),
    COUNTRY             VARCHAR(100),
    POSTAL_CODE         VARCHAR(20),
    REGION_ID           VARCHAR(20),
    REGISTRATION_DATE   DATE,
    CREDIT_LIMIT        NUMBER(12,2),
    LOYALTY_TIER        VARCHAR(20),
    PREFERRED_CHANNEL   VARCHAR(30),
    IS_ACTIVE           BOOLEAN,
    -- CDC classification
    CDC_OPERATION       VARCHAR(10),  -- INSERT|UPDATE|DELETE|UPSERT
    SRC_ROW_HASH        VARCHAR(64),  -- hash from Bronze
    PREV_HASH           VARCHAR(64),  -- last known hash from CDC_STATE
    IS_CHANGED          BOOLEAN,      -- TRUE if hash differs
    CHANGED_COLS        VARIANT,      -- JSON: ['email','loyalty_tier']
    -- SCD classification (determined after hash compare)
    SCD_ACTION          VARCHAR(20),  -- NEW_RECORD|EXPIRE_AND_INSERT|SCD1_UPDATE|NO_CHANGE
    STREAM_ACTION       VARCHAR(10),  -- INSERT|DELETE (from METADATA$ACTION)
    IS_UPDATE           BOOLEAN,      -- from METADATA$ISUPDATE
    -- Metadata
    LOADED_FROM_FILE    VARCHAR(500),
    STAGED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Product CDC staging
CREATE OR REPLACE TRANSIENT TABLE CDC_DB.STAGING.STG_PRODUCT_CHANGES (
    STAGE_ID        NUMBER AUTOINCREMENT,
    BATCH_ID        VARCHAR(100),
    PRODUCT_ID      VARCHAR(50) NOT NULL,
    PRODUCT_NAME    VARCHAR(300),
    CATEGORY        VARCHAR(100),
    SUB_CATEGORY    VARCHAR(100),
    BRAND           VARCHAR(100),
    UNIT_COST       NUMBER(10,2),
    UNIT_PRICE      NUMBER(10,2),
    SUPPLIER_ID     VARCHAR(50),
    IS_ACTIVE       BOOLEAN,
    LAUNCH_DATE     DATE,
    WEIGHT_KG       NUMBER(8,3),
    CDC_OPERATION   VARCHAR(10),
    SRC_ROW_HASH    VARCHAR(64),
    PREV_HASH       VARCHAR(64),
    IS_CHANGED      BOOLEAN,
    CHANGED_COLS    VARIANT,
    SCD_ACTION      VARCHAR(20),
    STREAM_ACTION   VARCHAR(10),
    IS_UPDATE       BOOLEAN,
    LOADED_FROM_FILE VARCHAR(500),
    STAGED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Employee CDC staging
CREATE OR REPLACE TRANSIENT TABLE CDC_DB.STAGING.STG_EMPLOYEE_CHANGES (
    STAGE_ID        NUMBER AUTOINCREMENT,
    BATCH_ID        VARCHAR(100),
    EMPLOYEE_ID     VARCHAR(50) NOT NULL,
    EMPLOYEE_NAME   VARCHAR(200),
    EMAIL           VARCHAR(200),
    DEPARTMENT      VARCHAR(100),
    JOB_TITLE       VARCHAR(100),
    MANAGER_ID      VARCHAR(50),
    STORE_ID        VARCHAR(50),
    HIRE_DATE       DATE,
    SALARY          NUMBER(12,2),
    IS_ACTIVE       BOOLEAN,
    CDC_OPERATION   VARCHAR(10),
    SRC_ROW_HASH    VARCHAR(64),
    PREV_HASH       VARCHAR(64),
    IS_CHANGED      BOOLEAN,
    CHANGED_COLS    VARIANT,
    SCD_ACTION      VARCHAR(20),
    STREAM_ACTION   VARCHAR(10),
    IS_UPDATE       BOOLEAN,
    LOADED_FROM_FILE VARCHAR(500),
    STAGED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ─────────────────────────────────────────────────────────────
-- SECTION 5: CDC CLASSIFICATION PROCEDURE
-- Reads streams, compares hashes, classifies each row as:
--   NEW_RECORD      → first time we see this business key
--   EXPIRE_AND_INSERT → existing record changed → needs SCD2 versioning
--   SCD1_UPDATE     → change to a Type 1 column → overwrite in place
--   DELETE_FLAG     → source row was deleted → set is_active=false
--   NO_CHANGE       → hash matches → skip entirely (most rows)
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE CDC_DB.CONTROL.SP_CLASSIFY_CUSTOMER_CHANGES(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_staged  NUMBER DEFAULT 0;
BEGIN
    -- Clear previous batch staging
    TRUNCATE TABLE CDC_DB.STAGING.STG_CUSTOMER_CHANGES;

    -- ── Read stream and classify every change ──────────────────
    INSERT INTO CDC_DB.STAGING.STG_CUSTOMER_CHANGES (
        BATCH_ID, CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE,
        SEGMENT, CITY, STATE, COUNTRY, POSTAL_CODE, REGION_ID,
        REGISTRATION_DATE, CREDIT_LIMIT, LOYALTY_TIER,
        PREFERRED_CHANNEL, IS_ACTIVE,
        CDC_OPERATION, SRC_ROW_HASH, PREV_HASH,
        IS_CHANGED, CHANGED_COLS, SCD_ACTION,
        STREAM_ACTION, IS_UPDATE, LOADED_FROM_FILE
    )
    SELECT
        :p_batch_id,
        s.CUSTOMER_ID,
        TRIM(s.CUSTOMER_NAME),
        LOWER(TRIM(s.EMAIL)),
        TRIM(s.PHONE),
        INITCAP(TRIM(s.SEGMENT)),
        TRIM(s.CITY),
        TRIM(s.STATE),
        TRIM(s.COUNTRY),
        TRIM(s.POSTAL_CODE),
        TRIM(s.REGION_ID),
        TRY_TO_DATE(s.REGISTRATION_DATE,'YYYY-MM-DD'),
        TRY_TO_NUMBER(s.CREDIT_LIMIT),
        UPPER(TRIM(s.LOYALTY_TIER)),
        TRIM(s.PREFERRED_CHANNEL),
        UPPER(TRIM(s.IS_ACTIVE)) IN ('TRUE','YES','Y','1','ACTIVE'),

        -- CDC Operation classification
        CASE
            WHEN s.METADATA$ACTION = 'DELETE' AND s.METADATA$ISUPDATE = FALSE
                THEN 'DELETE'
            WHEN s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = TRUE
                THEN 'UPDATE'
            WHEN s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = FALSE
                THEN 'INSERT'
            ELSE 'UPSERT'
        END AS CDC_OPERATION,

        s._SRC_ROW_HASH,
        ctrl.LAST_HASH,                           -- previous hash (NULL = new)

        -- Change detection: hash comparison
        CASE
            WHEN ctrl.LAST_HASH IS NULL THEN TRUE  -- new record
            WHEN ctrl.LAST_HASH != s._SRC_ROW_HASH THEN TRUE  -- changed
            ELSE FALSE
        END AS IS_CHANGED,

        -- Identify changed columns (compare old vs new values)
        CASE WHEN ctrl.LAST_HASH IS NULL THEN NULL
        ELSE
            ARRAY_CONSTRUCT_COMPACT(
                CASE WHEN LOWER(TRIM(s.EMAIL))          != COALESCE(dim.EMAIL,'')          THEN 'email'           END,
                CASE WHEN UPPER(TRIM(s.LOYALTY_TIER))   != COALESCE(dim.LOYALTY_TIER,'')   THEN 'loyalty_tier'    END,
                CASE WHEN TRIM(s.PREFERRED_CHANNEL)     != COALESCE(dim.PREFERRED_CHANNEL,'') THEN 'preferred_channel' END,
                CASE WHEN INITCAP(TRIM(s.SEGMENT))      != COALESCE(dim.SEGMENT,'')        THEN 'segment'         END,
                CASE WHEN TRIM(s.CITY)                  != COALESCE(dim.CITY,'')           THEN 'city'            END,
                CASE WHEN TRIM(s.STATE)                 != COALESCE(dim.STATE,'')          THEN 'state'           END,
                CASE WHEN TRY_TO_NUMBER(s.CREDIT_LIMIT) != COALESCE(dim.CREDIT_LIMIT,0)   THEN 'credit_limit'    END,
                CASE WHEN UPPER(TRIM(s.IS_ACTIVE)) IN ('TRUE','YES','Y','1')
                          != COALESCE(dim.IS_ACTIVE,TRUE)                                  THEN 'is_active'       END
            )
        END AS CHANGED_COLS,

        -- SCD Action classification
        CASE
            -- Brand new customer never seen before
            WHEN ctrl.LAST_HASH IS NULL
                THEN 'NEW_RECORD'
            -- Source row deleted
            WHEN s.METADATA$ACTION = 'DELETE' AND s.METADATA$ISUPDATE = FALSE
                THEN 'DELETE_FLAG'
            -- Hash matches — nothing changed
            WHEN ctrl.LAST_HASH = s._SRC_ROW_HASH
                THEN 'NO_CHANGE'
            -- SCD2 attributes changed (loyalty_tier, preferred_channel, segment)
            WHEN (
                UPPER(TRIM(s.LOYALTY_TIER))     != COALESCE(dim.LOYALTY_TIER,'')   OR
                TRIM(s.PREFERRED_CHANNEL)        != COALESCE(dim.PREFERRED_CHANNEL,'') OR
                INITCAP(TRIM(s.SEGMENT))         != COALESCE(dim.SEGMENT,'')
            )
                THEN 'EXPIRE_AND_INSERT'  -- SCD Type 2: create new version
            -- SCD1 attributes changed (email, address, phone — overwrite)
            WHEN ctrl.LAST_HASH != s._SRC_ROW_HASH
                THEN 'SCD1_UPDATE'        -- SCD Type 1: update in place
            ELSE 'NO_CHANGE'
        END AS SCD_ACTION,

        s.METADATA$ACTION,
        s.METADATA$ISUPDATE,
        s._LOAD_FILE

    FROM CDC_DB.STREAMS.STR_CUSTOMERS s

    -- Join to CDC state to get last known hash
    LEFT JOIN CDC_DB.CONTROL.CDC_STATE ctrl
        ON ctrl.TABLE_NAME = 'RAW_CUSTOMERS'
       AND ctrl.BUSINESS_KEY = s.CUSTOMER_ID

    -- Join to current Silver dimension to compare attribute values
    LEFT JOIN SILVER_DB.DIMENSIONS.DIM_CUSTOMERS dim
        ON dim.CUSTOMER_ID = s.CUSTOMER_ID
       AND dim.IS_CURRENT  = TRUE

    -- Only process the latest version per customer per batch
    -- (handles duplicate keys in same file)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.CUSTOMER_ID
        ORDER BY
            s.METADATA$ISUPDATE DESC,  -- prefer updates over pure inserts
            s._LOAD_TS DESC
    ) = 1;

    -- Log to event log
    INSERT INTO CDC_DB.CONTROL.CDC_EVENT_LOG (
        BATCH_ID, TABLE_NAME, OPERATION, BUSINESS_KEY,
        BEFORE_HASH, AFTER_HASH, CHANGED_COLUMNS, SCD_ACTION
    )
    SELECT
        BATCH_ID, 'RAW_CUSTOMERS', CDC_OPERATION, CUSTOMER_ID,
        PREV_HASH, SRC_ROW_HASH, CHANGED_COLS, SCD_ACTION
    FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES
    WHERE IS_CHANGED = TRUE OR CDC_OPERATION = 'DELETE';

    RETURN OBJECT_CONSTRUCT(
        'status',       'SUCCESS',
        'batch_id',     :p_batch_id,
        'rows_staged',  (SELECT COUNT(*) FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES),
        'new',          (SELECT COUNT(*) FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES WHERE SCD_ACTION = 'NEW_RECORD'),
        'expire_insert',(SELECT COUNT(*) FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES WHERE SCD_ACTION = 'EXPIRE_AND_INSERT'),
        'scd1_update',  (SELECT COUNT(*) FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES WHERE SCD_ACTION = 'SCD1_UPDATE'),
        'deleted',      (SELECT COUNT(*) FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES WHERE SCD_ACTION = 'DELETE_FLAG'),
        'no_change',    (SELECT COUNT(*) FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES WHERE SCD_ACTION = 'NO_CHANGE'),
        'duration_sec', DATEDIFF('second', :v_start, CURRENT_TIMESTAMP())
    );
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM,'batch_id',:p_batch_id);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- SECTION 6: CDC STATE UPDATE PROCEDURE
-- After Silver is updated, sync CDC_STATE with new hashes
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE CDC_DB.CONTROL.SP_SYNC_CDC_STATE(
    p_table_name VARCHAR,
    p_batch_id   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    -- Update CDC state with new hashes for processed rows
    MERGE INTO CDC_DB.CONTROL.CDC_STATE tgt
    USING (
        SELECT 'RAW_CUSTOMERS' AS TABLE_NAME,
               CUSTOMER_ID     AS BUSINESS_KEY,
               SRC_ROW_HASH    AS LAST_HASH,
               STAGED_AT       AS LAST_SEEN_AT
        FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES
        WHERE SCD_ACTION != 'NO_CHANGE'
    ) src
    ON tgt.TABLE_NAME = src.TABLE_NAME AND tgt.BUSINESS_KEY = src.BUSINESS_KEY
    WHEN MATCHED THEN UPDATE SET
        tgt.LAST_HASH       = src.LAST_HASH,
        tgt.LAST_SEEN_AT    = src.LAST_SEEN_AT,
        tgt.LAST_UPDATED_AT = CURRENT_TIMESTAMP(),
        tgt.CURRENT_VERSION = tgt.CURRENT_VERSION + 1
    WHEN NOT MATCHED THEN INSERT (TABLE_NAME, BUSINESS_KEY, LAST_HASH,
                                   FIRST_SEEN_AT, LAST_SEEN_AT)
    VALUES (src.TABLE_NAME, src.BUSINESS_KEY, src.LAST_HASH,
            src.LAST_SEEN_AT, src.LAST_SEEN_AT);

    RETURN 'CDC_STATE synced for batch: ' || p_batch_id;
END;
$$;
