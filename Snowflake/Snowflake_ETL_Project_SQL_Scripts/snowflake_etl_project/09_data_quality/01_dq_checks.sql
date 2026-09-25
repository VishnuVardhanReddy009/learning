-- ============================================================
-- FILE: 09_data_quality/01_dq_checks.sql
-- PURPOSE: Data quality rules, validation, alerting
-- RUN AS: DATA_ENGINEER_ROLE
-- ============================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE TRANSFORM_WH;

-- ─────────────────────────────────────────────────────────────
-- PROCEDURE: Run all DQ checks and log results
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE COMMON_DB.UTILITIES.SP_RUN_DQ_CHECKS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_fail_count NUMBER DEFAULT 0;
BEGIN

    -- ── CHECK 1: Bronze Orders — NULL order_id ─────────────────
    INSERT INTO COMMON_DB.AUDIT.DATA_QUALITY_LOG
        (TABLE_NAME, CHECK_NAME, CHECK_TYPE, ROWS_CHECKED, ROWS_FAILED, PASS_RATE_PCT, STATUS, DETAILS)
    SELECT
        'BRONZE_DB.RAW.RAW_ORDERS',
        'NULL_ORDER_ID',
        'NULL_CHECK',
        COUNT(*),
        SUM(CASE WHEN ORDER_ID IS NULL OR TRIM(ORDER_ID) = '' THEN 1 ELSE 0 END),
        ROUND((1 - SUM(CASE WHEN ORDER_ID IS NULL OR TRIM(ORDER_ID) = '' THEN 1 ELSE 0 END)
               / NULLIF(COUNT(*),0)) * 100, 2),
        CASE WHEN SUM(CASE WHEN ORDER_ID IS NULL OR TRIM(ORDER_ID) = '' THEN 1 ELSE 0 END) = 0
             THEN 'PASS' ELSE 'FAIL' END,
        OBJECT_CONSTRUCT('check', 'order_id must not be null', 'table', 'raw_orders')
    FROM BRONZE_DB.RAW.RAW_ORDERS
    WHERE _LOAD_TIMESTAMP >= DATEADD('hour', -25, CURRENT_TIMESTAMP());

    -- ── CHECK 2: Bronze Orders — Invalid order_date ────────────
    INSERT INTO COMMON_DB.AUDIT.DATA_QUALITY_LOG
        (TABLE_NAME, CHECK_NAME, CHECK_TYPE, ROWS_CHECKED, ROWS_FAILED, PASS_RATE_PCT, STATUS, DETAILS)
    SELECT
        'BRONZE_DB.RAW.RAW_ORDERS',
        'INVALID_ORDER_DATE',
        'FORMAT_CHECK',
        COUNT(*),
        SUM(CASE WHEN TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD') IS NULL THEN 1 ELSE 0 END),
        ROUND((1 - SUM(CASE WHEN TRY_TO_DATE(ORDER_DATE,'YYYY-MM-DD') IS NULL THEN 1 ELSE 0 END)
               / NULLIF(COUNT(*),0)) * 100, 2),
        CASE WHEN SUM(CASE WHEN TRY_TO_DATE(ORDER_DATE,'YYYY-MM-DD') IS NULL THEN 1 ELSE 0 END) = 0
             THEN 'PASS' ELSE 'WARN' END,
        OBJECT_CONSTRUCT('check', 'order_date must be YYYY-MM-DD format')
    FROM BRONZE_DB.RAW.RAW_ORDERS
    WHERE _LOAD_TIMESTAMP >= DATEADD('hour', -25, CURRENT_TIMESTAMP());

    -- ── CHECK 3: Bronze Orders — Negative amount ───────────────
    INSERT INTO COMMON_DB.AUDIT.DATA_QUALITY_LOG
        (TABLE_NAME, CHECK_NAME, CHECK_TYPE, ROWS_CHECKED, ROWS_FAILED, PASS_RATE_PCT, STATUS, DETAILS)
    SELECT
        'BRONZE_DB.RAW.RAW_ORDERS',
        'NEGATIVE_AMOUNT',
        'RANGE_CHECK',
        COUNT(*),
        SUM(CASE WHEN TRY_TO_NUMBER(TOTAL_AMOUNT) < 0 THEN 1 ELSE 0 END),
        ROUND((1 - SUM(CASE WHEN TRY_TO_NUMBER(TOTAL_AMOUNT) < 0 THEN 1 ELSE 0 END)
               / NULLIF(COUNT(*),0)) * 100, 2),
        CASE WHEN SUM(CASE WHEN TRY_TO_NUMBER(TOTAL_AMOUNT) < 0 THEN 1 ELSE 0 END) = 0
             THEN 'PASS' ELSE 'FAIL' END,
        OBJECT_CONSTRUCT('check', 'total_amount must be >= 0',
                         'min_allowed', 0, 'max_allowed', 999999)
    FROM BRONZE_DB.RAW.RAW_ORDERS
    WHERE _LOAD_TIMESTAMP >= DATEADD('hour', -25, CURRENT_TIMESTAMP());

    -- ── CHECK 4: Silver Facts — Orphaned orders (no customer) ──
    INSERT INTO COMMON_DB.AUDIT.DATA_QUALITY_LOG
        (TABLE_NAME, CHECK_NAME, CHECK_TYPE, ROWS_CHECKED, ROWS_FAILED, PASS_RATE_PCT, STATUS, DETAILS)
    SELECT
        'SILVER_DB.FACTS.FACT_ORDERS',
        'ORPHANED_CUSTOMER_FK',
        'REFERENTIAL',
        COUNT(*),
        SUM(CASE WHEN SK_CUSTOMER IS NULL THEN 1 ELSE 0 END),
        ROUND((1 - SUM(CASE WHEN SK_CUSTOMER IS NULL THEN 1 ELSE 0 END)
               / NULLIF(COUNT(*),0)) * 100, 2),
        CASE WHEN SUM(CASE WHEN SK_CUSTOMER IS NULL THEN 1 ELSE 0 END) = 0
             THEN 'PASS' ELSE 'WARN' END,
        OBJECT_CONSTRUCT('check', 'every order must have a matching customer in dim')
    FROM SILVER_DB.FACTS.FACT_ORDERS
    WHERE DW_CREATED_AT >= DATEADD('hour', -25, CURRENT_TIMESTAMP());

    -- ── CHECK 5: Silver DIM_CUSTOMERS — Duplicate current rows ─
    INSERT INTO COMMON_DB.AUDIT.DATA_QUALITY_LOG
        (TABLE_NAME, CHECK_NAME, CHECK_TYPE, ROWS_CHECKED, ROWS_FAILED, PASS_RATE_PCT, STATUS, DETAILS)
    WITH DUPES AS (
        SELECT CUSTOMER_ID, COUNT(*) AS cnt
        FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
        WHERE IS_CURRENT = TRUE
        GROUP BY CUSTOMER_ID
        HAVING cnt > 1
    )
    SELECT
        'SILVER_DB.DIMENSIONS.DIM_CUSTOMERS',
        'DUPLICATE_CURRENT_RECORDS',
        'UNIQUENESS',
        (SELECT COUNT(*) FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS WHERE IS_CURRENT=TRUE),
        COUNT(*),
        ROUND((1 - COUNT(*) / NULLIF(
            (SELECT COUNT(*) FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS WHERE IS_CURRENT=TRUE), 0
        )) * 100, 2),
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        OBJECT_CONSTRUCT('check', 'each customer must have exactly one IS_CURRENT=TRUE row',
                         'duplicate_customers', ARRAY_AGG(CUSTOMER_ID))
    FROM DUPES;

    -- ── CHECK 6: Gold daily summary — row count regression ─────
    INSERT INTO COMMON_DB.AUDIT.DATA_QUALITY_LOG
        (TABLE_NAME, CHECK_NAME, CHECK_TYPE, ROWS_CHECKED, ROWS_FAILED, PASS_RATE_PCT, STATUS, DETAILS)
    WITH TODAY_COUNT AS (
        SELECT COUNT(*) AS today_rows
        FROM GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY
        WHERE FULL_DATE = CURRENT_DATE() - 1
    ), YESTERDAY_COUNT AS (
        SELECT COUNT(*) AS yesterday_rows
        FROM GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY
        WHERE FULL_DATE = CURRENT_DATE() - 2
    )
    SELECT
        'GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY',
        'ROW_COUNT_REGRESSION',
        'VOLUME_CHECK',
        t.today_rows,
        CASE WHEN t.today_rows < y.yesterday_rows * 0.5 THEN 1 ELSE 0 END,
        100.0,
        CASE WHEN t.today_rows >= y.yesterday_rows * 0.5 THEN 'PASS'
             WHEN t.today_rows >= y.yesterday_rows * 0.2 THEN 'WARN'
             ELSE 'FAIL' END,
        OBJECT_CONSTRUCT('today_rows', t.today_rows, 'yesterday_rows', y.yesterday_rows,
                         'check', 'today row count must be >= 50% of yesterday')
    FROM TODAY_COUNT t, YESTERDAY_COUNT y;

    -- Count failures
    SELECT COUNT(*) INTO v_fail_count
    FROM COMMON_DB.AUDIT.DATA_QUALITY_LOG
    WHERE STATUS = 'FAIL'
      AND CHECK_DATE = CURRENT_DATE();

    RETURN OBJECT_CONSTRUCT(
        'procedure',        'SP_RUN_DQ_CHECKS',
        'status',           CASE WHEN v_fail_count > 0 THEN 'FAILED' ELSE 'PASSED' END,
        'checks_run',       6,
        'failures',         v_fail_count,
        'duration_sec',     DATEDIFF('second', v_start, CURRENT_TIMESTAMP()),
        'completed_at',     CURRENT_TIMESTAMP()::VARCHAR
    );
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- TASK: Run DQ checks daily before Gold pipeline
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_RUN_DQ_CHECKS
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_LOAD_FACT_ORDERS
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT           = 'Run data quality checks before Gold build'
AS
    CALL COMMON_DB.UTILITIES.SP_RUN_DQ_CHECKS();

ALTER TASK COMMON_DB.UTILITIES.TASK_RUN_DQ_CHECKS RESUME;

-- ─────────────────────────────────────────────────────────────
-- DQ DASHBOARD QUERIES
-- ─────────────────────────────────────────────────────────────

-- Today's DQ summary
SELECT
    TABLE_NAME,
    CHECK_NAME,
    CHECK_TYPE,
    ROWS_CHECKED,
    ROWS_FAILED,
    PASS_RATE_PCT,
    STATUS
FROM COMMON_DB.AUDIT.DATA_QUALITY_LOG
WHERE CHECK_DATE = CURRENT_DATE()
ORDER BY STATUS DESC, TABLE_NAME;

-- DQ trend over last 30 days
SELECT
    CHECK_DATE,
    COUNT(*)                                          AS TOTAL_CHECKS,
    SUM(CASE WHEN STATUS = 'PASS' THEN 1 ELSE 0 END) AS PASSED,
    SUM(CASE WHEN STATUS = 'WARN' THEN 1 ELSE 0 END) AS WARNED,
    SUM(CASE WHEN STATUS = 'FAIL' THEN 1 ELSE 0 END) AS FAILED,
    ROUND(SUM(CASE WHEN STATUS = 'PASS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS PASS_RATE
FROM COMMON_DB.AUDIT.DATA_QUALITY_LOG
WHERE CHECK_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1 ORDER BY 1 DESC;
