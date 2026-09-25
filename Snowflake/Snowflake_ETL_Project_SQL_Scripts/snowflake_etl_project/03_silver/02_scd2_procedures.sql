-- ============================================================
-- FILE: 03_silver/02_scd2_procedures.sql
-- PURPOSE: Stored procedures for Bronze→Silver transforms
--          SCD2 for dimensions, upsert for facts
-- LAYER: BRONZE → SILVER
-- RUN AS: DATA_ENGINEER_ROLE
-- ============================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE TRANSFORM_WH;

-- ─────────────────────────────────────────────────────────────
-- PROCEDURE: SP_LOAD_DIM_REGIONS (SCD Type 1 — simple upsert)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SILVER_DB.DIMENSIONS.SP_LOAD_DIM_REGIONS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_inserted  NUMBER DEFAULT 0;
    v_updated   NUMBER DEFAULT 0;
    v_start     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- SCD Type 1: MERGE (upsert) — overwrite changes, no history
    MERGE INTO SILVER_DB.DIMENSIONS.DIM_REGIONS tgt
    USING (
        SELECT DISTINCT
            REGION_ID,
            REGION_NAME,
            COUNTRY,
            COUNTRY_CODE,
            TIMEZONE,
            SALES_TERRITORY,
            _LOAD_FILE
        FROM BRONZE_DB.RAW.RAW_REGIONS
        WHERE REGION_ID IS NOT NULL
          AND TRIM(REGION_ID) != ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY REGION_ID ORDER BY _LOAD_TIMESTAMP DESC) = 1
    ) src
    ON tgt.REGION_ID = src.REGION_ID
    WHEN MATCHED AND (
        tgt.REGION_NAME     != src.REGION_NAME     OR
        tgt.COUNTRY         != src.COUNTRY         OR
        tgt.SALES_TERRITORY != src.SALES_TERRITORY
    ) THEN UPDATE SET
        tgt.REGION_NAME     = src.REGION_NAME,
        tgt.COUNTRY         = src.COUNTRY,
        tgt.COUNTRY_CODE    = src.COUNTRY_CODE,
        tgt.TIMEZONE        = src.TIMEZONE,
        tgt.SALES_TERRITORY = src.SALES_TERRITORY,
        tgt.DW_UPDATED_AT   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        REGION_ID, REGION_NAME, COUNTRY, COUNTRY_CODE, TIMEZONE, SALES_TERRITORY
    ) VALUES (
        src.REGION_ID, src.REGION_NAME, src.COUNTRY, src.COUNTRY_CODE,
        src.TIMEZONE, src.SALES_TERRITORY
    );

    -- Capture row counts
    v_inserted := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
                   WHERE "number of rows inserted" > 0);

    RETURN OBJECT_CONSTRUCT(
        'procedure',    'SP_LOAD_DIM_REGIONS',
        'status',       'SUCCESS',
        'duration_sec', DATEDIFF('second', v_start, CURRENT_TIMESTAMP()),
        'completed_at', CURRENT_TIMESTAMP()::VARCHAR
    );
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'procedure', 'SP_LOAD_DIM_REGIONS',
            'status',    'FAILED',
            'error',     SQLERRM
        );
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- PROCEDURE: SP_LOAD_DIM_CUSTOMERS (SCD Type 2)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SILVER_DB.DIMENSIONS.SP_LOAD_DIM_CUSTOMERS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_expired   NUMBER DEFAULT 0;
    v_inserted  NUMBER DEFAULT 0;
BEGIN
    -- ── Step 1: Expire changed current records ─────────────────
    -- Mark existing current rows as inactive when source has changed
    UPDATE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS tgt
    SET
        EFF_END_DATE = CURRENT_DATE() - 1,
        IS_CURRENT   = FALSE,
        DW_UPDATED_AT = CURRENT_TIMESTAMP()
    FROM (
        -- Identify customers with changed attributes
        SELECT src.CUSTOMER_ID
        FROM (
            SELECT
                CUSTOMER_ID,
                TRIM(CUSTOMER_NAME)    AS CUSTOMER_NAME,
                LOWER(TRIM(EMAIL))     AS EMAIL,
                TRIM(SEGMENT)          AS SEGMENT,
                TRIM(CITY)             AS CITY,
                TRIM(STATE)            AS STATE,
                TRIM(COUNTRY)          AS COUNTRY,
                TRIM(REGION_ID)        AS REGION_ID,
                TRY_TO_NUMBER(CREDIT_LIMIT) AS CREDIT_LIMIT
            FROM BRONZE_DB.RAW.RAW_CUSTOMERS
            WHERE CUSTOMER_ID IS NOT NULL
              AND _IS_PROCESSED = FALSE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY _LOAD_TIMESTAMP DESC) = 1
        ) src
        JOIN SILVER_DB.DIMENSIONS.DIM_CUSTOMERS dim
            ON src.CUSTOMER_ID = dim.CUSTOMER_ID
           AND dim.IS_CURRENT  = TRUE
        WHERE
            COALESCE(src.CUSTOMER_NAME, '') != COALESCE(dim.CUSTOMER_NAME, '')  OR
            COALESCE(src.EMAIL,          '') != COALESCE(dim.EMAIL,          '')  OR
            COALESCE(src.SEGMENT,        '') != COALESCE(dim.SEGMENT,        '')  OR
            COALESCE(src.CITY,           '') != COALESCE(dim.CITY,           '')  OR
            COALESCE(src.REGION_ID,      '') != COALESCE(dim.REGION_ID,      '')  OR
            COALESCE(src.CREDIT_LIMIT,    0) != COALESCE(dim.CREDIT_LIMIT,    0)
    ) changed
    WHERE tgt.CUSTOMER_ID = changed.CUSTOMER_ID
      AND tgt.IS_CURRENT  = TRUE;

    -- ── Step 2: Insert new current records (changed + net-new) ──
    INSERT INTO SILVER_DB.DIMENSIONS.DIM_CUSTOMERS (
        CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, SEGMENT,
        CITY, STATE, COUNTRY, POSTAL_CODE, REGION_ID,
        REGISTRATION_DATE, CREDIT_LIMIT,
        EFF_START_DATE, EFF_END_DATE, IS_CURRENT, DW_SOURCE_FILE
    )
    SELECT
        src.CUSTOMER_ID,
        TRIM(src.CUSTOMER_NAME),
        LOWER(TRIM(src.EMAIL)),
        TRIM(src.PHONE),
        INITCAP(TRIM(src.SEGMENT)),
        TRIM(src.CITY),
        TRIM(src.STATE),
        TRIM(src.COUNTRY),
        TRIM(src.POSTAL_CODE),
        TRIM(src.REGION_ID),
        TRY_TO_DATE(src.REGISTRATION_DATE, 'YYYY-MM-DD'),
        TRY_TO_NUMBER(src.CREDIT_LIMIT),
        CURRENT_DATE()          AS EFF_START_DATE,
        NULL                    AS EFF_END_DATE,
        TRUE                    AS IS_CURRENT,
        src._LOAD_FILE
    FROM (
        SELECT *
        FROM BRONZE_DB.RAW.RAW_CUSTOMERS
        WHERE CUSTOMER_ID IS NOT NULL
          AND _IS_PROCESSED = FALSE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY _LOAD_TIMESTAMP DESC) = 1
    ) src
    WHERE
        -- Net new customers
        NOT EXISTS (
            SELECT 1 FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS dim
            WHERE dim.CUSTOMER_ID = src.CUSTOMER_ID AND dim.IS_CURRENT = TRUE
        )
        OR
        -- Changed customers (existing row just expired above)
        NOT EXISTS (
            SELECT 1 FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS dim
            WHERE dim.CUSTOMER_ID = src.CUSTOMER_ID
              AND dim.EFF_START_DATE = CURRENT_DATE()
              AND dim.IS_CURRENT = TRUE
        );

    -- ── Step 3: Mark source rows as processed ──────────────────
    UPDATE BRONZE_DB.RAW.RAW_CUSTOMERS
    SET _IS_PROCESSED = TRUE
    WHERE _IS_PROCESSED = FALSE;

    RETURN OBJECT_CONSTRUCT(
        'procedure',    'SP_LOAD_DIM_CUSTOMERS',
        'status',       'SUCCESS',
        'duration_sec', DATEDIFF('second', v_start, CURRENT_TIMESTAMP()),
        'completed_at', CURRENT_TIMESTAMP()::VARCHAR
    );
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'procedure', 'SP_LOAD_DIM_CUSTOMERS',
            'status',    'FAILED',
            'error',     SQLERRM
        );
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- PROCEDURE: SP_LOAD_DIM_PRODUCTS (SCD Type 2)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SILVER_DB.DIMENSIONS.SP_LOAD_DIM_PRODUCTS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- Expire changed records
    UPDATE SILVER_DB.DIMENSIONS.DIM_PRODUCTS tgt
    SET EFF_END_DATE = CURRENT_DATE() - 1, IS_CURRENT = FALSE, DW_UPDATED_AT = CURRENT_TIMESTAMP()
    FROM (
        SELECT src.PRODUCT_ID
        FROM (
            SELECT PRODUCT_ID,
                TRY_TO_NUMBER(UNIT_COST) AS UNIT_COST,
                TRY_TO_NUMBER(UNIT_PRICE) AS UNIT_PRICE,
                IS_ACTIVE, BRAND
            FROM BRONZE_DB.RAW.RAW_PRODUCTS WHERE _IS_PROCESSED = FALSE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY _LOAD_TIMESTAMP DESC) = 1
        ) src
        JOIN SILVER_DB.DIMENSIONS.DIM_PRODUCTS dim
            ON src.PRODUCT_ID = dim.PRODUCT_ID AND dim.IS_CURRENT = TRUE
        WHERE COALESCE(src.UNIT_COST,0)  != COALESCE(dim.UNIT_COST,0)
           OR COALESCE(src.UNIT_PRICE,0) != COALESCE(dim.UNIT_PRICE,0)
           OR COALESCE(src.BRAND,'')     != COALESCE(dim.BRAND,'')
    ) changed
    WHERE tgt.PRODUCT_ID = changed.PRODUCT_ID AND tgt.IS_CURRENT = TRUE;

    -- Insert new / changed records
    INSERT INTO SILVER_DB.DIMENSIONS.DIM_PRODUCTS (
        PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, BRAND,
        UNIT_COST, UNIT_PRICE, SUPPLIER_ID, IS_ACTIVE, LAUNCH_DATE,
        EFF_START_DATE, EFF_END_DATE, IS_CURRENT, DW_SOURCE_FILE
    )
    SELECT
        PRODUCT_ID,
        TRIM(PRODUCT_NAME),
        INITCAP(TRIM(CATEGORY)),
        INITCAP(TRIM(SUB_CATEGORY)),
        TRIM(BRAND),
        TRY_TO_NUMBER(UNIT_COST),
        TRY_TO_NUMBER(UNIT_PRICE),
        TRIM(SUPPLIER_ID),
        UPPER(TRIM(IS_ACTIVE)) IN ('TRUE','YES','Y','1'),
        TRY_TO_DATE(LAUNCH_DATE, 'YYYY-MM-DD'),
        CURRENT_DATE(), NULL, TRUE, _LOAD_FILE
    FROM BRONZE_DB.RAW.RAW_PRODUCTS
    WHERE _IS_PROCESSED = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY _LOAD_TIMESTAMP DESC) = 1
    AND NOT EXISTS (
        SELECT 1 FROM SILVER_DB.DIMENSIONS.DIM_PRODUCTS dim
        WHERE dim.PRODUCT_ID = BRONZE_DB.RAW.RAW_PRODUCTS.PRODUCT_ID
          AND dim.IS_CURRENT = TRUE
          AND dim.EFF_START_DATE = CURRENT_DATE()
    );

    UPDATE BRONZE_DB.RAW.RAW_PRODUCTS SET _IS_PROCESSED = TRUE WHERE _IS_PROCESSED = FALSE;

    RETURN OBJECT_CONSTRUCT('procedure','SP_LOAD_DIM_PRODUCTS','status','SUCCESS',
        'duration_sec', DATEDIFF('second',v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('procedure','SP_LOAD_DIM_PRODUCTS','status','FAILED','error',SQLERRM);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- PROCEDURE: SP_LOAD_FACT_ORDERS
-- Transforms raw orders into Silver fact with FK resolution
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE SILVER_DB.FACTS.SP_LOAD_FACT_ORDERS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted  NUMBER DEFAULT 0;
    v_rejected  NUMBER DEFAULT 0;
BEGIN
    -- Insert into SILVER FACT (upsert using MERGE for idempotency)
    MERGE INTO SILVER_DB.FACTS.FACT_ORDERS tgt
    USING (
        SELECT
            o.ORDER_ID,
            -- Resolve customer SK (use current record)
            c.SK_CUSTOMER,
            -- Resolve region SK
            r.SK_REGION,
            -- Date keys
            TRY_TO_NUMBER(TO_CHAR(TRY_TO_DATE(o.ORDER_DATE,'YYYY-MM-DD'),'YYYYMMDD')) AS ORDER_DATE_KEY,
            TRY_TO_NUMBER(TO_CHAR(TRY_TO_DATE(o.SHIP_DATE, 'YYYY-MM-DD'),'YYYYMMDD')) AS SHIP_DATE_KEY,
            -- Measures (cast and validate)
            COALESCE(TRY_TO_NUMBER(o.TOTAL_AMOUNT), 0)  AS TOTAL_AMOUNT,
            COALESCE(TRY_TO_NUMBER(o.DISCOUNT_PCT), 0)  AS DISCOUNT_PCT,
            COALESCE(TRY_TO_NUMBER(o.PROFIT), 0)        AS PROFIT,
            -- Descriptive
            UPPER(TRIM(o.STATUS))    AS STATUS,
            INITCAP(TRIM(o.SHIP_MODE)) AS SHIP_MODE,
            o._LOAD_FILE             AS DW_SOURCE_FILE
        FROM BRONZE_DB.RAW.RAW_ORDERS o
        -- Resolve customer SK
        LEFT JOIN SILVER_DB.DIMENSIONS.DIM_CUSTOMERS c
            ON c.CUSTOMER_ID = o.CUSTOMER_ID AND c.IS_CURRENT = TRUE
        -- Resolve region SK
        LEFT JOIN SILVER_DB.DIMENSIONS.DIM_REGIONS r
            ON r.REGION_ID = o.REGION_ID
        WHERE o.ORDER_ID IS NOT NULL
          AND o._IS_PROCESSED = FALSE
          AND TRY_TO_DATE(o.ORDER_DATE, 'YYYY-MM-DD') IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY o.ORDER_ID ORDER BY o._LOAD_TIMESTAMP DESC) = 1
    ) src
    ON tgt.ORDER_ID = src.ORDER_ID
    WHEN MATCHED AND (
        tgt.STATUS != src.STATUS OR tgt.SHIP_DATE_KEY != src.SHIP_DATE_KEY
    ) THEN UPDATE SET
        tgt.STATUS        = src.STATUS,
        tgt.SHIP_DATE_KEY = src.SHIP_DATE_KEY,
        tgt.DW_UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, SK_CUSTOMER, SK_REGION, ORDER_DATE_KEY, SHIP_DATE_KEY,
        TOTAL_AMOUNT, DISCOUNT_PCT, PROFIT, STATUS, SHIP_MODE, DW_SOURCE_FILE
    ) VALUES (
        src.ORDER_ID, src.SK_CUSTOMER, src.SK_REGION, src.ORDER_DATE_KEY,
        src.SHIP_DATE_KEY, src.TOTAL_AMOUNT, src.DISCOUNT_PCT, src.PROFIT,
        src.STATUS, src.SHIP_MODE, src.DW_SOURCE_FILE
    );

    -- Mark processed
    UPDATE BRONZE_DB.RAW.RAW_ORDERS SET _IS_PROCESSED = TRUE WHERE _IS_PROCESSED = FALSE;

    RETURN OBJECT_CONSTRUCT('procedure','SP_LOAD_FACT_ORDERS','status','SUCCESS',
        'duration_sec', DATEDIFF('second',v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('procedure','SP_LOAD_FACT_ORDERS','status','FAILED','error',SQLERRM);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- TEST PROCEDURES
-- ─────────────────────────────────────────────────────────────
CALL SILVER_DB.DIMENSIONS.SP_LOAD_DIM_REGIONS();
CALL SILVER_DB.DIMENSIONS.SP_LOAD_DIM_CUSTOMERS();
CALL SILVER_DB.DIMENSIONS.SP_LOAD_DIM_PRODUCTS();
CALL SILVER_DB.FACTS.SP_LOAD_FACT_ORDERS();

-- Verify SCD2 history
SELECT CUSTOMER_ID, CUSTOMER_NAME, EMAIL, EFF_START_DATE, EFF_END_DATE, IS_CURRENT
FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
ORDER BY CUSTOMER_ID, EFF_START_DATE;
