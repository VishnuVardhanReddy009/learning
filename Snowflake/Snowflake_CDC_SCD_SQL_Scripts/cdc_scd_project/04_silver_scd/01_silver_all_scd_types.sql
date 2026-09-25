-- ============================================================
-- FILE: 04_silver_scd/01_silver_all_scd_types.sql
-- PURPOSE: All four SCD types — tables, procedures, examples
--
-- SCD TYPE 0 — Fixed / Retain Original (DIM_DATE, DIM_REGIONS)
-- SCD TYPE 1 — Overwrite (DIM_STORES — latest value, no history)
-- SCD TYPE 2 — Add Row (DIM_CUSTOMERS, DIM_PRODUCTS, DIM_EMPLOYEES)
-- SCD TYPE 3 — Add Column (DIM_CUSTOMERS — previous segment)
-- SCD TYPE 4 — History Table (DIM_PRODUCT_PRICE_HISTORY)
-- SCD TYPE 6 — Hybrid 1+2+3 (DIM_CUSTOMERS — combines all approaches)
-- ============================================================

USE ROLE DATA_ENG_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE SILVER_DB;

-- ═══════════════════════════════════════════════════════════════
-- SCD TYPE 0 — FIXED / RETAIN ORIGINAL
-- The original value is always kept. Never overwritten.
-- Use for: date dimension, immutable reference data
-- ═══════════════════════════════════════════════════════════════

USE SCHEMA DIMENSIONS;

CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_KEY        NUMBER        NOT NULL,   -- YYYYMMDD (surrogate + natural key)
    FULL_DATE       DATE          NOT NULL,
    DAY_OF_WEEK     NUMBER(1),
    DAY_NAME        VARCHAR(10),
    DAY_OF_MONTH    NUMBER(2),
    DAY_OF_YEAR     NUMBER(3),
    WEEK_OF_YEAR    NUMBER(2),
    MONTH_NUM       NUMBER(2),
    MONTH_NAME      VARCHAR(10),
    QUARTER         NUMBER(1),
    YEAR            NUMBER(4),
    IS_WEEKEND      BOOLEAN,
    IS_HOLIDAY      BOOLEAN DEFAULT FALSE,
    FISCAL_YEAR     NUMBER(4),
    FISCAL_QUARTER  NUMBER(1),
    -- SCD0: once loaded, these values NEVER change
    -- Even if the company changes fiscal year start, history is preserved
    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_KEY)
) COMMENT='SCD Type 0 — fixed values, never overwritten';

-- Populate date dimension (2018–2030)
INSERT INTO DIM_DATE
WITH SPINE AS (
    SELECT DATEADD('day', SEQ4(), '2018-01-01'::DATE) AS D
    FROM TABLE(GENERATOR(ROWCOUNT=>4748))
)
SELECT
    TO_NUMBER(TO_CHAR(D,'YYYYMMDD'))           DATE_KEY,
    D                                           FULL_DATE,
    DAYOFWEEKISO(D)                             DAY_OF_WEEK,
    DAYNAME(D)                                  DAY_NAME,
    DAYOFMONTH(D)                               DAY_OF_MONTH,
    DAYOFYEAR(D)                                DAY_OF_YEAR,
    WEEKOFYEAR(D)                               WEEK_OF_YEAR,
    MONTH(D)                                    MONTH_NUM,
    MONTHNAME(D)                                MONTH_NAME,
    QUARTER(D)                                  QUARTER,
    YEAR(D)                                     YEAR,
    DAYOFWEEKISO(D) IN (6,7)                    IS_WEEKEND,
    FALSE                                       IS_HOLIDAY,
    CASE WHEN MONTH(D)>=4 THEN YEAR(D)+1 ELSE YEAR(D) END FISCAL_YEAR,
    CASE WHEN MONTH(D) IN (4,5,6)    THEN 1
         WHEN MONTH(D) IN (7,8,9)    THEN 2
         WHEN MONTH(D) IN (10,11,12) THEN 3
         ELSE 4 END                             FISCAL_QUARTER
FROM SPINE;

-- ═══════════════════════════════════════════════════════════════
-- SCD TYPE 1 — OVERWRITE (UPSERT)
-- Latest value replaces old value. No history preserved.
-- Use for: stores, regions — where history is not analytically needed
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS DIM_STORES (
    SK_STORE        NUMBER DEFAULT COMMON_DB.UTILITIES.SEQ_STORE_SK.NEXTVAL,
    STORE_ID        VARCHAR(50)   NOT NULL,      -- natural key
    STORE_NAME      VARCHAR(200),
    STORE_TYPE      VARCHAR(50),
    CITY            VARCHAR(100),
    STATE           VARCHAR(100),
    COUNTRY         VARCHAR(100),
    REGION_ID       VARCHAR(20),
    OPEN_DATE       DATE,
    MANAGER_ID      VARCHAR(50),
    IS_ACTIVE       BOOLEAN,
    -- SCD1 audit columns
    DW_CREATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE  VARCHAR(500),
    CONSTRAINT PK_DIM_STORES PRIMARY KEY (SK_STORE),
    CONSTRAINT UQ_STORE_ID   UNIQUE (STORE_ID)
) COMMENT='SCD Type 1 — latest value overwrites, no history';

-- SCD1 Procedure: MERGE (UPSERT)
CREATE OR REPLACE PROCEDURE SILVER_DB.DIMENSIONS.SP_SCD1_STORES(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- SCD Type 1 = simple MERGE — UPDATE when matched, INSERT when new
    MERGE INTO SILVER_DB.DIMENSIONS.DIM_STORES tgt
    USING (
        SELECT
            STORE_ID, STORE_NAME, STORE_TYPE, CITY, STATE, COUNTRY,
            REGION_ID, TRY_TO_DATE(OPEN_DATE,'YYYY-MM-DD') AS OPEN_DATE,
            MANAGER_ID,
            UPPER(TRIM(IS_ACTIVE)) IN ('TRUE','YES','Y','1') AS IS_ACTIVE,
            _LOAD_FILE
        FROM CDC_DB.STREAMS.STR_STORES
        WHERE STORE_ID IS NOT NULL
          AND METADATA$ACTION = 'INSERT'  -- only process inserts
        QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_ID ORDER BY _LOAD_TS DESC) = 1
    ) src
    ON tgt.STORE_ID = src.STORE_ID
    -- SCD1: ALWAYS update all columns when matched (no versioning)
    WHEN MATCHED THEN UPDATE SET
        tgt.STORE_NAME     = src.STORE_NAME,
        tgt.STORE_TYPE     = src.STORE_TYPE,
        tgt.CITY           = src.CITY,
        tgt.STATE          = src.STATE,
        tgt.COUNTRY        = src.COUNTRY,
        tgt.REGION_ID      = src.REGION_ID,
        tgt.OPEN_DATE      = src.OPEN_DATE,
        tgt.MANAGER_ID     = src.MANAGER_ID,
        tgt.IS_ACTIVE      = src.IS_ACTIVE,
        tgt.DW_UPDATED_AT  = CURRENT_TIMESTAMP(),
        tgt.DW_SOURCE_FILE = src._LOAD_FILE
    WHEN NOT MATCHED THEN INSERT (
        STORE_ID, STORE_NAME, STORE_TYPE, CITY, STATE, COUNTRY,
        REGION_ID, OPEN_DATE, MANAGER_ID, IS_ACTIVE, DW_SOURCE_FILE
    ) VALUES (
        src.STORE_ID, src.STORE_NAME, src.STORE_TYPE, src.CITY, src.STATE,
        src.COUNTRY, src.REGION_ID, src.OPEN_DATE, src.MANAGER_ID,
        src.IS_ACTIVE, src._LOAD_FILE
    );

    RETURN OBJECT_CONSTRUCT('procedure','SP_SCD1_STORES','status','SUCCESS',
        'duration_sec',DATEDIFF('second',:v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM);
END;
$$;

-- ═══════════════════════════════════════════════════════════════
-- SCD TYPE 2 — ADD NEW ROW (Full History)
-- When an attribute changes: expire old row, insert new row
-- Use for: customers (segment, loyalty), products (price),
--          employees (department, title, salary)
-- Key columns: EFF_START_DATE, EFF_END_DATE, IS_CURRENT, VERSION_NUM
-- ═══════════════════════════════════════════════════════════════

-- ── DIM_CUSTOMERS — SCD Type 2 + Type 6 hybrid ───────────────
CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS (
    SK_CUSTOMER         NUMBER DEFAULT COMMON_DB.UTILITIES.SEQ_CUSTOMER_SK.NEXTVAL,
    CUSTOMER_ID         VARCHAR(50)  NOT NULL,   -- natural key (same across versions)
    -- Type 2 tracked attributes (create new row on change)
    SEGMENT             VARCHAR(30),
    LOYALTY_TIER        VARCHAR(20),
    PREFERRED_CHANNEL   VARCHAR(30),
    -- Type 1 tracked attributes (overwrite in place, keep in current row only)
    CUSTOMER_NAME       VARCHAR(200),
    EMAIL               VARCHAR(200),
    PHONE               VARCHAR(30),
    CITY                VARCHAR(100),
    STATE               VARCHAR(100),
    COUNTRY             VARCHAR(100),
    POSTAL_CODE         VARCHAR(20),
    REGION_ID           VARCHAR(20),
    CREDIT_LIMIT        NUMBER(12,2),
    REGISTRATION_DATE   DATE,
    IS_ACTIVE           BOOLEAN,
    -- Type 3: keep PREVIOUS value for key attribute (Hybrid Type 6)
    PREV_SEGMENT        VARCHAR(30),    -- previous segment value (Type 3)
    PREV_LOYALTY_TIER   VARCHAR(20),    -- previous loyalty tier (Type 3)
    -- SCD2 versioning columns
    EFF_START_DATE      DATE          NOT NULL,
    EFF_END_DATE        DATE,               -- NULL = current/active record
    IS_CURRENT          BOOLEAN       DEFAULT TRUE,
    VERSION_NUM         NUMBER        DEFAULT 1,
    -- CDC metadata
    CDC_OPERATION       VARCHAR(10),        -- INSERT|UPDATE|DELETE
    SCD_ACTION          VARCHAR(20),        -- NEW_RECORD|EXPIRE_AND_INSERT|SCD1_UPDATE
    CHANGED_COLS        VARIANT,            -- JSON array of changed column names
    -- DW metadata
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    DW_BATCH_ID         VARCHAR(100),
    CONSTRAINT PK_DIM_CUSTOMERS PRIMARY KEY (SK_CUSTOMER)
)
CLUSTER BY (CUSTOMER_ID, IS_CURRENT)
COMMENT = 'SCD Type 2+6 customer dimension — full segment and loyalty history';

-- ── DIM_PRODUCTS — SCD Type 2 (price tracking) ───────────────
CREATE TABLE IF NOT EXISTS DIM_PRODUCTS (
    SK_PRODUCT          NUMBER DEFAULT COMMON_DB.UTILITIES.SEQ_PRODUCT_SK.NEXTVAL,
    PRODUCT_ID          VARCHAR(50)  NOT NULL,
    -- Type 2 tracked (price changes create new version)
    UNIT_COST           NUMBER(10,2),
    UNIT_PRICE          NUMBER(10,2),
    CATEGORY            VARCHAR(100),
    SUB_CATEGORY        VARCHAR(100),
    -- Type 1 tracked (overwrite)
    PRODUCT_NAME        VARCHAR(300),
    BRAND               VARCHAR(100),
    SUPPLIER_ID         VARCHAR(50),
    IS_ACTIVE           BOOLEAN,
    LAUNCH_DATE         DATE,
    WEIGHT_KG           NUMBER(8,3),
    -- Derived columns (virtual — auto-computed)
    MARGIN_PCT          NUMBER(7,4) AS (
                            CASE WHEN UNIT_PRICE > 0
                                 THEN ((UNIT_PRICE - UNIT_COST)/UNIT_PRICE)*100
                                 ELSE NULL END),
    PRICE_TIER          VARCHAR(20) AS (
                            CASE WHEN UNIT_PRICE <  25  THEN 'Budget'
                                 WHEN UNIT_PRICE <  100 THEN 'Standard'
                                 WHEN UNIT_PRICE <  500 THEN 'Premium'
                                 ELSE 'Luxury' END),
    -- SCD2 versioning
    EFF_START_DATE      DATE         NOT NULL,
    EFF_END_DATE        DATE,
    IS_CURRENT          BOOLEAN      DEFAULT TRUE,
    VERSION_NUM         NUMBER       DEFAULT 1,
    -- Price change tracking
    PRICE_CHANGE_PCT    NUMBER(8,4),    -- % change from previous price
    PREV_UNIT_PRICE     NUMBER(10,2),   -- Type 3: keep previous price
    -- CDC + DW metadata
    CDC_OPERATION       VARCHAR(10),
    SCD_ACTION          VARCHAR(20),
    CHANGED_COLS        VARIANT,
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    DW_BATCH_ID         VARCHAR(100),
    CONSTRAINT PK_DIM_PRODUCTS PRIMARY KEY (SK_PRODUCT)
)
CLUSTER BY (PRODUCT_ID, IS_CURRENT)
COMMENT = 'SCD Type 2 product dimension — full price and category history';

-- ── DIM_EMPLOYEES — SCD Type 2 (org changes) ─────────────────
CREATE TABLE IF NOT EXISTS DIM_EMPLOYEES (
    SK_EMPLOYEE         NUMBER DEFAULT COMMON_DB.UTILITIES.SEQ_EMPLOYEE_SK.NEXTVAL,
    EMPLOYEE_ID         VARCHAR(50)  NOT NULL,
    EMPLOYEE_NAME       VARCHAR(200),
    EMAIL               VARCHAR(200),
    -- Type 2 tracked (org changes need history for attribution)
    DEPARTMENT          VARCHAR(100),   -- which dept made which sales
    JOB_TITLE           VARCHAR(100),   -- title at time of sale
    MANAGER_ID          VARCHAR(50),
    STORE_ID            VARCHAR(50),
    SALARY              NUMBER(12,2),
    -- Type 1 tracked
    HIRE_DATE           DATE,
    IS_ACTIVE           BOOLEAN,
    -- SCD2 versioning
    EFF_START_DATE      DATE         NOT NULL,
    EFF_END_DATE        DATE,
    IS_CURRENT          BOOLEAN      DEFAULT TRUE,
    VERSION_NUM         NUMBER       DEFAULT 1,
    -- CDC metadata
    CDC_OPERATION       VARCHAR(10),
    SCD_ACTION          VARCHAR(20),
    CHANGED_COLS        VARIANT,
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    DW_BATCH_ID         VARCHAR(100),
    CONSTRAINT PK_DIM_EMPLOYEES PRIMARY KEY (SK_EMPLOYEE)
)
CLUSTER BY (EMPLOYEE_ID, IS_CURRENT)
COMMENT = 'SCD Type 2 employee dimension — tracks org/role/dept history';

-- ═══════════════════════════════════════════════════════════════
-- SCD TYPE 4 — HISTORY TABLE (Separate table for all versions)
-- Keep current values in main table + all history in separate table
-- Use for: product prices (fast current lookup + complete audit trail)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS DIM_PRODUCT_PRICE_HISTORY (
    HISTORY_ID      NUMBER AUTOINCREMENT PRIMARY KEY,
    PRODUCT_ID      VARCHAR(50)  NOT NULL,
    UNIT_COST       NUMBER(10,2),
    UNIT_PRICE      NUMBER(10,2),
    MARGIN_PCT      NUMBER(7,4),
    VALID_FROM      DATE         NOT NULL,
    VALID_TO        DATE,               -- NULL = still current price
    IS_CURRENT      BOOLEAN,
    CHANGE_REASON   VARCHAR(200),       -- why price changed
    CHANGED_BY      VARCHAR(100) DEFAULT CURRENT_USER(),
    DW_CREATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_BATCH_ID     VARCHAR(100)
) COMMENT='SCD Type 4 history table for product prices — separate from main dim';

-- ═══════════════════════════════════════════════════════════════
-- SCD TYPE 2 — COMPLETE PROCEDURE FOR DIM_CUSTOMERS
-- Implements the full expire-and-insert pattern using CDC staging
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE SILVER_DB.DIMENSIONS.SP_SCD2_CUSTOMERS(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_expired       NUMBER DEFAULT 0;
    v_inserted_new  NUMBER DEFAULT 0;
    v_scd1_updated  NUMBER DEFAULT 0;
    v_deleted       NUMBER DEFAULT 0;
BEGIN
    -- ══════════════════════════════════════════════════════════
    -- STEP 1: EXPIRE changed SCD2 records
    -- Set EFF_END_DATE = yesterday on rows that need versioning
    -- Only rows with EXPIRE_AND_INSERT action need this step
    -- ══════════════════════════════════════════════════════════
    UPDATE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS tgt
    SET
        EFF_END_DATE  = CURRENT_DATE() - 1,
        IS_CURRENT    = FALSE,
        DW_UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.CUSTOMER_ID IN (
        SELECT CUSTOMER_ID
        FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES
        WHERE SCD_ACTION = 'EXPIRE_AND_INSERT'
    )
    AND tgt.IS_CURRENT = TRUE;   -- only expire the current row

    -- ══════════════════════════════════════════════════════════
    -- STEP 2: INSERT new current rows
    -- For: NEW_RECORD (first time) + EXPIRE_AND_INSERT (new version)
    -- ══════════════════════════════════════════════════════════
    INSERT INTO SILVER_DB.DIMENSIONS.DIM_CUSTOMERS (
        CUSTOMER_ID, SEGMENT, LOYALTY_TIER, PREFERRED_CHANNEL,
        CUSTOMER_NAME, EMAIL, PHONE, CITY, STATE, COUNTRY,
        POSTAL_CODE, REGION_ID, CREDIT_LIMIT, REGISTRATION_DATE,
        IS_ACTIVE,
        -- Type 3: carry forward previous SCD2 values
        PREV_SEGMENT, PREV_LOYALTY_TIER,
        -- SCD2 versioning
        EFF_START_DATE, EFF_END_DATE, IS_CURRENT, VERSION_NUM,
        -- CDC metadata
        CDC_OPERATION, SCD_ACTION, CHANGED_COLS,
        DW_SOURCE_FILE, DW_BATCH_ID
    )
    SELECT
        chg.CUSTOMER_ID,
        chg.SEGMENT,
        chg.LOYALTY_TIER,
        chg.PREFERRED_CHANNEL,
        chg.CUSTOMER_NAME,
        chg.EMAIL,
        chg.PHONE,
        chg.CITY,
        chg.STATE,
        chg.COUNTRY,
        chg.POSTAL_CODE,
        chg.REGION_ID,
        chg.CREDIT_LIMIT,
        chg.REGISTRATION_DATE,
        chg.IS_ACTIVE,
        -- Type 3: previous values come from the EXPIRING row
        prev.SEGMENT        AS PREV_SEGMENT,
        prev.LOYALTY_TIER   AS PREV_LOYALTY_TIER,
        -- Effective dates
        CURRENT_DATE()      AS EFF_START_DATE,
        NULL                AS EFF_END_DATE,     -- NULL = current record
        TRUE                AS IS_CURRENT,
        -- Version = previous version + 1 (or 1 for new records)
        COALESCE(prev.VERSION_NUM, 0) + 1        AS VERSION_NUM,
        -- CDC metadata
        chg.CDC_OPERATION,
        chg.SCD_ACTION,
        chg.CHANGED_COLS,
        chg.LOADED_FROM_FILE,
        :p_batch_id
    FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES chg
    -- Get the row being expired (to copy Type 3 values)
    LEFT JOIN SILVER_DB.DIMENSIONS.DIM_CUSTOMERS prev
        ON prev.CUSTOMER_ID = chg.CUSTOMER_ID
       AND prev.IS_CURRENT  = FALSE
       AND prev.EFF_END_DATE = CURRENT_DATE() - 1   -- just expired by Step 1
    WHERE chg.SCD_ACTION IN ('NEW_RECORD', 'EXPIRE_AND_INSERT');

    -- ══════════════════════════════════════════════════════════
    -- STEP 3: SCD Type 1 UPDATE (in-place overwrite for Type 1 cols)
    -- For: email, phone, city, state, country, credit_limit
    -- These updates happen on the CURRENT row without versioning
    -- ══════════════════════════════════════════════════════════
    UPDATE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS tgt
    SET
        tgt.CUSTOMER_NAME  = src.CUSTOMER_NAME,
        tgt.EMAIL          = src.EMAIL,
        tgt.PHONE          = src.PHONE,
        tgt.CITY           = src.CITY,
        tgt.STATE          = src.STATE,
        tgt.COUNTRY        = src.COUNTRY,
        tgt.POSTAL_CODE    = src.POSTAL_CODE,
        tgt.CREDIT_LIMIT   = src.CREDIT_LIMIT,
        tgt.IS_ACTIVE      = src.IS_ACTIVE,
        tgt.CDC_OPERATION  = 'UPDATE',
        tgt.SCD_ACTION     = 'SCD1_UPDATE',
        tgt.CHANGED_COLS   = src.CHANGED_COLS,
        tgt.DW_UPDATED_AT  = CURRENT_TIMESTAMP(),
        tgt.DW_BATCH_ID    = :p_batch_id
    FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES src
    WHERE tgt.CUSTOMER_ID = src.CUSTOMER_ID
      AND tgt.IS_CURRENT  = TRUE
      AND src.SCD_ACTION  = 'SCD1_UPDATE';

    -- ══════════════════════════════════════════════════════════
    -- STEP 4: SOFT DELETE (mark is_active=false, don't physically delete)
    -- Source sent a DELETE signal — customer is no longer active
    -- Preserve history — never physically delete from DW
    -- ══════════════════════════════════════════════════════════
    UPDATE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS tgt
    SET
        tgt.IS_ACTIVE     = FALSE,
        tgt.CDC_OPERATION = 'DELETE',
        tgt.SCD_ACTION    = 'SOFT_DELETE',
        tgt.DW_UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE tgt.CUSTOMER_ID IN (
        SELECT CUSTOMER_ID FROM CDC_DB.STAGING.STG_CUSTOMER_CHANGES
        WHERE SCD_ACTION = 'DELETE_FLAG'
    )
    AND tgt.IS_CURRENT = TRUE;

    -- ══════════════════════════════════════════════════════════
    -- STEP 5: Sync CDC state after successful Silver update
    -- ══════════════════════════════════════════════════════════
    CALL CDC_DB.CONTROL.SP_SYNC_CDC_STATE('RAW_CUSTOMERS', :p_batch_id);

    RETURN OBJECT_CONSTRUCT(
        'procedure',    'SP_SCD2_CUSTOMERS',
        'status',       'SUCCESS',
        'batch_id',     :p_batch_id,
        'new_records',  (SELECT COUNT(*) FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS WHERE DW_BATCH_ID = :p_batch_id AND SCD_ACTION = 'NEW_RECORD'),
        'expire_insert',(SELECT COUNT(*) FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS WHERE DW_BATCH_ID = :p_batch_id AND SCD_ACTION = 'EXPIRE_AND_INSERT'),
        'scd1_updates', (SELECT COUNT(*) FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS WHERE DW_BATCH_ID = :p_batch_id AND SCD_ACTION = 'SCD1_UPDATE'),
        'duration_sec', DATEDIFF('second',:v_start,CURRENT_TIMESTAMP())
    );
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM,'batch_id',:p_batch_id);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- SCD TYPE 2 PROCEDURE — PRODUCTS
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SILVER_DB.DIMENSIONS.SP_SCD2_PRODUCTS(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- Step 1: Expire products with changed price/category
    UPDATE SILVER_DB.DIMENSIONS.DIM_PRODUCTS tgt
    SET EFF_END_DATE = CURRENT_DATE()-1, IS_CURRENT=FALSE, DW_UPDATED_AT=CURRENT_TIMESTAMP()
    WHERE PRODUCT_ID IN (
        SELECT PRODUCT_ID FROM CDC_DB.STAGING.STG_PRODUCT_CHANGES
        WHERE SCD_ACTION = 'EXPIRE_AND_INSERT'
    ) AND IS_CURRENT = TRUE;

    -- Step 2: Insert new current versions
    INSERT INTO SILVER_DB.DIMENSIONS.DIM_PRODUCTS (
        PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, BRAND,
        UNIT_COST, UNIT_PRICE, SUPPLIER_ID, IS_ACTIVE, LAUNCH_DATE, WEIGHT_KG,
        PREV_UNIT_PRICE, PRICE_CHANGE_PCT,
        EFF_START_DATE, EFF_END_DATE, IS_CURRENT, VERSION_NUM,
        CDC_OPERATION, SCD_ACTION, CHANGED_COLS, DW_SOURCE_FILE, DW_BATCH_ID
    )
    SELECT
        chg.PRODUCT_ID, chg.PRODUCT_NAME, chg.CATEGORY, chg.SUB_CATEGORY,
        chg.BRAND, chg.UNIT_COST, chg.UNIT_PRICE, chg.SUPPLIER_ID,
        chg.IS_ACTIVE, chg.LAUNCH_DATE, chg.WEIGHT_KG,
        -- Type 3: previous price
        prev.UNIT_PRICE                                         AS PREV_UNIT_PRICE,
        -- Price change percentage
        CASE WHEN COALESCE(prev.UNIT_PRICE,0) > 0
             THEN ((chg.UNIT_PRICE - prev.UNIT_PRICE)/prev.UNIT_PRICE)*100
             ELSE NULL END                                      AS PRICE_CHANGE_PCT,
        CURRENT_DATE(), NULL, TRUE,
        COALESCE(prev.VERSION_NUM,0)+1,
        chg.CDC_OPERATION, chg.SCD_ACTION, chg.CHANGED_COLS,
        chg.LOADED_FROM_FILE, :p_batch_id
    FROM CDC_DB.STAGING.STG_PRODUCT_CHANGES chg
    LEFT JOIN SILVER_DB.DIMENSIONS.DIM_PRODUCTS prev
        ON prev.PRODUCT_ID = chg.PRODUCT_ID
       AND prev.IS_CURRENT = FALSE AND prev.EFF_END_DATE = CURRENT_DATE()-1
    WHERE chg.SCD_ACTION IN ('NEW_RECORD','EXPIRE_AND_INSERT');

    -- Step 3: SCD1 update (name, brand, supplier overwrite)
    UPDATE SILVER_DB.DIMENSIONS.DIM_PRODUCTS tgt
    SET PRODUCT_NAME=src.PRODUCT_NAME, BRAND=src.BRAND,
        SUPPLIER_ID=src.SUPPLIER_ID, IS_ACTIVE=src.IS_ACTIVE,
        SCD_ACTION='SCD1_UPDATE', DW_UPDATED_AT=CURRENT_TIMESTAMP(), DW_BATCH_ID=:p_batch_id
    FROM CDC_DB.STAGING.STG_PRODUCT_CHANGES src
    WHERE tgt.PRODUCT_ID=src.PRODUCT_ID AND tgt.IS_CURRENT=TRUE AND src.SCD_ACTION='SCD1_UPDATE';

    -- Step 4: Write to Type 4 price history table
    INSERT INTO SILVER_DB.DIMENSIONS.DIM_PRODUCT_PRICE_HISTORY (
        PRODUCT_ID, UNIT_COST, UNIT_PRICE, VALID_FROM, VALID_TO, IS_CURRENT, DW_BATCH_ID
    )
    SELECT PRODUCT_ID, UNIT_COST, UNIT_PRICE, EFF_START_DATE, EFF_END_DATE, IS_CURRENT, :p_batch_id
    FROM SILVER_DB.DIMENSIONS.DIM_PRODUCTS
    WHERE DW_BATCH_ID = :p_batch_id
      AND SCD_ACTION IN ('NEW_RECORD','EXPIRE_AND_INSERT');

    -- Sync CDC state
    CALL CDC_DB.CONTROL.SP_SYNC_CDC_STATE('RAW_PRODUCTS', :p_batch_id);

    RETURN OBJECT_CONSTRUCT('procedure','SP_SCD2_PRODUCTS','status','SUCCESS',
        'duration_sec',DATEDIFF('second',:v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- SCD TYPE 2 PROCEDURE — EMPLOYEES
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SILVER_DB.DIMENSIONS.SP_SCD2_EMPLOYEES(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- Step 1: Expire employees with org changes
    UPDATE SILVER_DB.DIMENSIONS.DIM_EMPLOYEES tgt
    SET EFF_END_DATE=CURRENT_DATE()-1, IS_CURRENT=FALSE, DW_UPDATED_AT=CURRENT_TIMESTAMP()
    WHERE EMPLOYEE_ID IN (
        SELECT EMPLOYEE_ID FROM CDC_DB.STAGING.STG_EMPLOYEE_CHANGES
        WHERE SCD_ACTION='EXPIRE_AND_INSERT'
    ) AND IS_CURRENT=TRUE;

    -- Step 2: Insert new current versions
    INSERT INTO SILVER_DB.DIMENSIONS.DIM_EMPLOYEES (
        EMPLOYEE_ID,EMPLOYEE_NAME,EMAIL,DEPARTMENT,JOB_TITLE,
        MANAGER_ID,STORE_ID,HIRE_DATE,SALARY,IS_ACTIVE,
        EFF_START_DATE,EFF_END_DATE,IS_CURRENT,VERSION_NUM,
        CDC_OPERATION,SCD_ACTION,CHANGED_COLS,DW_SOURCE_FILE,DW_BATCH_ID
    )
    SELECT
        chg.EMPLOYEE_ID,chg.EMPLOYEE_NAME,chg.EMAIL,chg.DEPARTMENT,
        chg.JOB_TITLE,chg.MANAGER_ID,chg.STORE_ID,chg.HIRE_DATE,
        chg.SALARY,chg.IS_ACTIVE,
        CURRENT_DATE(),NULL,TRUE,COALESCE(prev.VERSION_NUM,0)+1,
        chg.CDC_OPERATION,chg.SCD_ACTION,chg.CHANGED_COLS,
        chg.LOADED_FROM_FILE,:p_batch_id
    FROM CDC_DB.STAGING.STG_EMPLOYEE_CHANGES chg
    LEFT JOIN SILVER_DB.DIMENSIONS.DIM_EMPLOYEES prev
        ON prev.EMPLOYEE_ID=chg.EMPLOYEE_ID AND prev.IS_CURRENT=FALSE
       AND prev.EFF_END_DATE=CURRENT_DATE()-1
    WHERE chg.SCD_ACTION IN ('NEW_RECORD','EXPIRE_AND_INSERT');

    RETURN OBJECT_CONSTRUCT('procedure','SP_SCD2_EMPLOYEES','status','SUCCESS',
        'duration_sec',DATEDIFF('second',:v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- FACT TABLES
-- ─────────────────────────────────────────────────────────────
USE SCHEMA FACTS;

CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    ORDER_SK            NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ID            VARCHAR(50)  NOT NULL,
    -- Foreign keys (surrogate keys at time of order)
    SK_CUSTOMER         NUMBER,
    SK_STORE            NUMBER,
    SK_EMPLOYEE         NUMBER,
    -- Date keys
    ORDER_DATE_KEY      NUMBER,
    SHIP_DATE_KEY       NUMBER,
    DELIVERY_DATE_KEY   NUMBER,
    -- Measures
    TOTAL_AMOUNT        NUMBER(12,2),
    DISCOUNT_PCT        NUMBER(6,3),
    DISCOUNT_AMOUNT     NUMBER(12,2) AS (TOTAL_AMOUNT * DISCOUNT_PCT / 100),
    NET_AMOUNT          NUMBER(12,2) AS (TOTAL_AMOUNT - (TOTAL_AMOUNT * DISCOUNT_PCT / 100)),
    PROFIT              NUMBER(12,2),
    -- Descriptive
    STATUS              VARCHAR(20),
    SHIP_MODE           VARCHAR(30),
    PAYMENT_METHOD      VARCHAR(30),
    DAYS_TO_SHIP        NUMBER AS (DATEDIFF('day',
        TO_DATE(ORDER_DATE_KEY::VARCHAR,'YYYYMMDD'),
        TO_DATE(SHIP_DATE_KEY::VARCHAR,'YYYYMMDD'))),
    -- DW metadata
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    DW_BATCH_ID         VARCHAR(100),
    CONSTRAINT UQ_ORDER_ID UNIQUE (ORDER_ID)
)
CLUSTER BY (ORDER_DATE_KEY, SK_CUSTOMER)
COMMENT='Fact table at order grain — FK to all SCD2 dims at time of order';

CREATE TABLE IF NOT EXISTS FACT_ORDER_ITEMS (
    ITEM_SK             NUMBER AUTOINCREMENT PRIMARY KEY,
    ITEM_ID             VARCHAR(50)  NOT NULL,
    ORDER_ID            VARCHAR(50),
    -- FK to point-in-time dimension versions
    SK_CUSTOMER         NUMBER,
    SK_PRODUCT          NUMBER,
    SK_STORE            NUMBER,
    SK_EMPLOYEE         NUMBER,
    ORDER_DATE_KEY      NUMBER,
    -- Measures
    QUANTITY            NUMBER,
    UNIT_PRICE          NUMBER(10,2),
    DISCOUNT_AMOUNT     NUMBER(10,2),
    LINE_TOTAL          NUMBER(12,2),
    RETURN_FLAG         BOOLEAN,
    RETURN_DATE         DATE,
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_SOURCE_FILE      VARCHAR(500),
    DW_BATCH_ID         VARCHAR(100),
    CONSTRAINT UQ_ITEM_ID UNIQUE (ITEM_ID)
)
CLUSTER BY (ORDER_DATE_KEY, SK_PRODUCT, SK_CUSTOMER)
COMMENT='Fact table at line-item grain — FK to point-in-time dim versions';

-- ─────────────────────────────────────────────────────────────
-- FACT LOADING PROCEDURE
-- Critical: resolves FK to the correct SCD2 version that was
-- active AT THE TIME the order was placed
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SILVER_DB.FACTS.SP_LOAD_FACT_ORDERS(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    MERGE INTO SILVER_DB.FACTS.FACT_ORDERS tgt
    USING (
        SELECT
            o.ORDER_ID,
            -- ══════════════════════════════════════════════════
            -- CRITICAL SCD2 FK RESOLUTION:
            -- We must find the dimension SK that was ACTIVE
            -- when the order was placed (not today's current SK)
            -- This is the "point-in-time" lookup pattern
            -- ══════════════════════════════════════════════════
            c.SK_CUSTOMER,   -- customer segment at time of order
            s.SK_STORE,
            e.SK_EMPLOYEE,   -- employee's department at time of order

            -- Date keys
            TRY_TO_NUMBER(TO_CHAR(TRY_TO_DATE(o.ORDER_DATE,'YYYY-MM-DD'),'YYYYMMDD'))    ORDER_DATE_KEY,
            TRY_TO_NUMBER(TO_CHAR(TRY_TO_DATE(o.SHIP_DATE,'YYYY-MM-DD'),'YYYYMMDD'))     SHIP_DATE_KEY,
            TRY_TO_NUMBER(TO_CHAR(TRY_TO_DATE(o.DELIVERY_DATE,'YYYY-MM-DD'),'YYYYMMDD')) DELIVERY_DATE_KEY,

            -- Measures
            COALESCE(TRY_TO_NUMBER(o.TOTAL_AMOUNT), 0)  TOTAL_AMOUNT,
            COALESCE(TRY_TO_NUMBER(o.DISCOUNT_PCT), 0)  DISCOUNT_PCT,
            COALESCE(TRY_TO_NUMBER(o.PROFIT), 0)        PROFIT,
            UPPER(TRIM(o.STATUS))                        STATUS,
            INITCAP(TRIM(o.SHIP_MODE))                   SHIP_MODE,
            TRIM(o.PAYMENT_METHOD)                       PAYMENT_METHOD,
            o._LOAD_FILE                                 DW_SOURCE_FILE
        FROM CDC_DB.STREAMS.STR_ORDERS o

        -- Point-in-time customer FK resolution:
        -- Find which SCD2 version of customer was active on the order date
        LEFT JOIN SILVER_DB.DIMENSIONS.DIM_CUSTOMERS c
            ON c.CUSTOMER_ID = o.CUSTOMER_ID
           AND TRY_TO_DATE(o.ORDER_DATE,'YYYY-MM-DD')
                   BETWEEN c.EFF_START_DATE
                   AND COALESCE(c.EFF_END_DATE, '9999-12-31')

        -- Store FK (SCD1 — just use current record)
        LEFT JOIN SILVER_DB.DIMENSIONS.DIM_STORES s
            ON s.STORE_ID = o.STORE_ID

        -- Point-in-time employee FK resolution
        LEFT JOIN SILVER_DB.DIMENSIONS.DIM_EMPLOYEES e
            ON e.EMPLOYEE_ID = o.EMPLOYEE_ID
           AND TRY_TO_DATE(o.ORDER_DATE,'YYYY-MM-DD')
                   BETWEEN e.EFF_START_DATE
                   AND COALESCE(e.EFF_END_DATE, '9999-12-31')

        WHERE o.ORDER_ID IS NOT NULL
          AND TRY_TO_DATE(o.ORDER_DATE,'YYYY-MM-DD') IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY o.ORDER_ID ORDER BY o._LOAD_TS DESC) = 1
    ) src
    ON tgt.ORDER_ID = src.ORDER_ID
    WHEN MATCHED AND tgt.STATUS != src.STATUS THEN UPDATE SET
        tgt.STATUS        = src.STATUS,
        tgt.SHIP_DATE_KEY = src.SHIP_DATE_KEY,
        tgt.DW_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, SK_CUSTOMER, SK_STORE, SK_EMPLOYEE,
        ORDER_DATE_KEY, SHIP_DATE_KEY, DELIVERY_DATE_KEY,
        TOTAL_AMOUNT, DISCOUNT_PCT, PROFIT,
        STATUS, SHIP_MODE, PAYMENT_METHOD, DW_SOURCE_FILE, DW_BATCH_ID
    ) VALUES (
        src.ORDER_ID, src.SK_CUSTOMER, src.SK_STORE, src.SK_EMPLOYEE,
        src.ORDER_DATE_KEY, src.SHIP_DATE_KEY, src.DELIVERY_DATE_KEY,
        src.TOTAL_AMOUNT, src.DISCOUNT_PCT, src.PROFIT,
        src.STATUS, src.SHIP_MODE, src.PAYMENT_METHOD, src.DW_SOURCE_FILE, :p_batch_id
    );

    RETURN OBJECT_CONSTRUCT('procedure','SP_LOAD_FACT_ORDERS','status','SUCCESS',
        'duration_sec',DATEDIFF('second',:v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM);
END;
$$;
