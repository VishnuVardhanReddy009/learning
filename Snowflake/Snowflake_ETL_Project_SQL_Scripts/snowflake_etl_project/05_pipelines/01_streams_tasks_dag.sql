-- ============================================================
-- FILE: 05_pipelines/01_streams_tasks_dag.sql
-- PURPOSE: Streams for CDC + Task DAG for full pipeline
-- PIPELINE: Bronze → Silver → Gold (automated end-to-end)
-- RUN AS: DATA_ENGINEER_ROLE
-- ============================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE TRANSFORM_WH;

-- ─────────────────────────────────────────────────────────────
-- SECTION A: STREAMS on Bronze tables
-- Track new rows arriving from Snowpipe / COPY INTO
-- ─────────────────────────────────────────────────────────────

-- Append-only streams — Bronze tables are insert-only (never updated)
CREATE STREAM IF NOT EXISTS BRONZE_DB.RAW.STREAM_RAW_ORDERS
    ON TABLE BRONZE_DB.RAW.RAW_ORDERS
    APPEND_ONLY = TRUE           -- more efficient for insert-only source
    COMMENT = 'CDC stream on raw orders — detects new rows from COPY INTO/Snowpipe';

CREATE STREAM IF NOT EXISTS BRONZE_DB.RAW.STREAM_RAW_CUSTOMERS
    ON TABLE BRONZE_DB.RAW.RAW_CUSTOMERS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream on raw customers';

CREATE STREAM IF NOT EXISTS BRONZE_DB.RAW.STREAM_RAW_PRODUCTS
    ON TABLE BRONZE_DB.RAW.RAW_PRODUCTS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream on raw products';

-- Standard stream on Silver dimensions (tracks SCD2 changes)
CREATE STREAM IF NOT EXISTS SILVER_DB.DIMENSIONS.STREAM_DIM_CUSTOMERS
    ON TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    COMMENT = 'CDC stream on Silver dim_customers — feeds Gold customer mart';

-- Check stream health
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE_DB.RAW.STREAM_RAW_ORDERS') AS has_orders;
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE_DB.RAW.STREAM_RAW_CUSTOMERS') AS has_customers;

-- ─────────────────────────────────────────────────────────────
-- SECTION B: TASK DAG — Full pipeline orchestration
--
-- DAG structure:
--
-- TASK_ROOT (scheduler, every 1hr)
--     │
--     ├── TASK_LOAD_SILVER_DIMS (parallel)
--     │       ├── TASK_LOAD_DIM_REGIONS
--     │       ├── TASK_LOAD_DIM_CUSTOMERS
--     │       └── TASK_LOAD_DIM_PRODUCTS
--     │
--     ├── TASK_LOAD_SILVER_FACTS (after dims complete)
--     │       ├── TASK_LOAD_FACT_ORDERS
--     │       └── TASK_LOAD_FACT_ITEMS
--     │
--     ├── TASK_BUILD_GOLD (after facts complete)
--     │       ├── TASK_BUILD_DAILY_SUMMARY
--     │       └── TASK_BUILD_CUSTOMER_360
--     │
--     └── TASK_NOTIFY_SUCCESS (after all gold complete)
--
-- ─────────────────────────────────────────────────────────────

-- ── ROOT TASK: Scheduler ──────────────────────────────────────
-- Only runs if at least one Bronze stream has data
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT
    WAREHOUSE         = TRANSFORM_WH
    SCHEDULE          = 'USING CRON 0 * * * * UTC'    -- every hour at :00
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    USER_TASK_TIMEOUT_MS = 3600000                     -- 1 hour max
    COMMENT           = 'Root task — hourly pipeline trigger'
    WHEN
        SYSTEM$STREAM_HAS_DATA('BRONZE_DB.RAW.STREAM_RAW_ORDERS')    OR
        SYSTEM$STREAM_HAS_DATA('BRONZE_DB.RAW.STREAM_RAW_CUSTOMERS') OR
        SYSTEM$STREAM_HAS_DATA('BRONZE_DB.RAW.STREAM_RAW_PRODUCTS')
AS
    -- Log pipeline start
    INSERT INTO COMMON_DB.AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, STAGE, STATUS, STARTED_AT)
    VALUES
        (UUID_STRING(), 'PIPELINE_START', 'RUNNING', CURRENT_TIMESTAMP());

-- ── DIM REGIONS task ─────────────────────────────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_LOAD_DIM_REGIONS
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT           = 'Load Silver dim_regions from Bronze'
AS
    CALL SILVER_DB.DIMENSIONS.SP_LOAD_DIM_REGIONS();

-- ── DIM CUSTOMERS task ───────────────────────────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_LOAD_DIM_CUSTOMERS
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT           = 'Load Silver dim_customers (SCD2) from Bronze'
AS
    CALL SILVER_DB.DIMENSIONS.SP_LOAD_DIM_CUSTOMERS();

-- ── DIM PRODUCTS task ────────────────────────────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_LOAD_DIM_PRODUCTS
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT           = 'Load Silver dim_products (SCD2) from Bronze'
AS
    CALL SILVER_DB.DIMENSIONS.SP_LOAD_DIM_PRODUCTS();

-- ── FACT ORDERS task (after ALL dims complete) ────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_LOAD_FACT_ORDERS
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_LOAD_DIM_REGIONS,
                      COMMON_DB.UTILITIES.TASK_LOAD_DIM_CUSTOMERS,
                      COMMON_DB.UTILITIES.TASK_LOAD_DIM_PRODUCTS
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT           = 'Load Silver fact_orders after dims complete'
AS
    CALL SILVER_DB.FACTS.SP_LOAD_FACT_ORDERS();

-- ── GOLD DAILY SUMMARY task ───────────────────────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_BUILD_DAILY_SUMMARY
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_LOAD_FACT_ORDERS
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT           = 'Build Gold daily sales summary'
AS
    CALL GOLD_DB.SALES_MART.SP_BUILD_DAILY_SALES_SUMMARY();

-- ── GOLD CUSTOMER 360 task ────────────────────────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_BUILD_CUSTOMER_360
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_LOAD_FACT_ORDERS
    ERROR_INTEGRATION = SNS_PIPELINE_ALERTS
    COMMENT           = 'Build Gold customer 360 with RFM scoring'
AS
    CALL GOLD_DB.CUSTOMER_MART.SP_BUILD_CUSTOMER_360();

-- ── SUCCESS NOTIFICATION task ────────────────────────────────
CREATE OR REPLACE TASK COMMON_DB.UTILITIES.TASK_NOTIFY_SUCCESS
    WAREHOUSE         = TRANSFORM_WH
    AFTER             COMMON_DB.UTILITIES.TASK_BUILD_DAILY_SUMMARY,
                      COMMON_DB.UTILITIES.TASK_BUILD_CUSTOMER_360
    COMMENT           = 'Send success notification after full pipeline completes'
AS
    INSERT INTO COMMON_DB.AUDIT.PIPELINE_RUN_LOG
        (STAGE, STATUS, COMPLETED_AT, ROW_COUNT)
    SELECT
        'PIPELINE_COMPLETE',
        'SUCCESS',
        CURRENT_TIMESTAMP(),
        (SELECT COUNT(*) FROM GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY
         WHERE DW_UPDATED_AT >= DATEADD('hour',-2,CURRENT_TIMESTAMP()));

-- ─────────────────────────────────────────────────────────────
-- RESUME TASKS — Must resume leaf tasks FIRST, root LAST
-- ─────────────────────────────────────────────────────────────
ALTER TASK COMMON_DB.UTILITIES.TASK_NOTIFY_SUCCESS      RESUME;
ALTER TASK COMMON_DB.UTILITIES.TASK_BUILD_CUSTOMER_360  RESUME;
ALTER TASK COMMON_DB.UTILITIES.TASK_BUILD_DAILY_SUMMARY RESUME;
ALTER TASK COMMON_DB.UTILITIES.TASK_LOAD_FACT_ORDERS    RESUME;
ALTER TASK COMMON_DB.UTILITIES.TASK_LOAD_DIM_PRODUCTS   RESUME;
ALTER TASK COMMON_DB.UTILITIES.TASK_LOAD_DIM_CUSTOMERS  RESUME;
ALTER TASK COMMON_DB.UTILITIES.TASK_LOAD_DIM_REGIONS    RESUME;
ALTER TASK COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT       RESUME;  -- LAST

-- ─────────────────────────────────────────────────────────────
-- MANUAL EXECUTION (for testing without waiting for schedule)
-- ─────────────────────────────────────────────────────────────
-- EXECUTE TASK COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT;

-- ─────────────────────────────────────────────────────────────
-- MONITORING
-- ─────────────────────────────────────────────────────────────
-- View task dependency graph
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT',
    RECURSIVE => TRUE
));

-- Task run history with status
SELECT
    NAME,
    STATE,
    ERROR_CODE,
    ERROR_MESSAGE,
    SCHEDULED_TIME,
    QUERY_START_TIME,
    COMPLETED_TIME,
    DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DURATION_SEC
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour',-24,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
ORDER BY SCHEDULED_TIME DESC;

-- Suspend entire pipeline
-- ALTER TASK COMMON_DB.UTILITIES.TASK_PIPELINE_ROOT SUSPEND;
