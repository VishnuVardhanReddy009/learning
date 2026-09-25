-- ============================================================
-- FILE: 04_gold/01_gold_tables_views.sql
-- PURPOSE: Gold layer — aggregations, marts, BI-ready views
-- LAYER: GOLD (Business)
-- RUN AS: DATA_ENGINEER_ROLE
-- ============================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE GOLD_DB;

-- ─────────────────────────────────────────────────────────────
-- SALES MART — Aggregated fact tables for BI
-- ─────────────────────────────────────────────────────────────
USE SCHEMA SALES_MART;

-- Daily sales summary (pre-aggregated for fast dashboards)
CREATE TABLE IF NOT EXISTS DAILY_SALES_SUMMARY (
    SUMMARY_KEY         NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_DATE_KEY      NUMBER,
    FULL_DATE           DATE,
    REGION_ID           VARCHAR(20),
    REGION_NAME         VARCHAR(100),
    COUNTRY             VARCHAR(100),
    SALES_TERRITORY     VARCHAR(100),
    ORDER_COUNT         NUMBER,
    UNIQUE_CUSTOMERS    NUMBER,
    TOTAL_REVENUE       NUMBER(14,2),
    TOTAL_DISCOUNT      NUMBER(14,2),
    NET_REVENUE         NUMBER(14,2),
    TOTAL_PROFIT        NUMBER(14,2),
    AVG_ORDER_VALUE     NUMBER(10,2),
    PROFIT_MARGIN_PCT   NUMBER(8,4),
    DW_CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ORDER_DATE_KEY, REGION_ID)
COMMENT = 'Daily sales summary by region — pre-aggregated for dashboards';

-- Monthly category performance
CREATE TABLE IF NOT EXISTS MONTHLY_CATEGORY_SALES (
    YEAR                NUMBER,
    MONTH_NUM           NUMBER,
    MONTH_NAME          VARCHAR(10),
    CATEGORY            VARCHAR(100),
    SUB_CATEGORY        VARCHAR(100),
    UNITS_SOLD          NUMBER,
    REVENUE             NUMBER(14,2),
    PROFIT              NUMBER(14,2),
    AVG_UNIT_PRICE      NUMBER(10,2),
    UNIQUE_CUSTOMERS    NUMBER,
    RETURN_COUNT        NUMBER,
    DW_UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (YEAR, CATEGORY)
COMMENT = 'Monthly category performance for product analytics';

-- ─────────────────────────────────────────────────────────────
-- CUSTOMER MART
-- ─────────────────────────────────────────────────────────────
USE SCHEMA CUSTOMER_MART;

-- Customer 360 — lifetime value and behavior
CREATE TABLE IF NOT EXISTS CUSTOMER_360 (
    SK_CUSTOMER             NUMBER,
    CUSTOMER_ID             VARCHAR(50),
    CUSTOMER_NAME           VARCHAR(200),
    EMAIL                   VARCHAR(200),   -- masked in views
    SEGMENT                 VARCHAR(30),
    REGION_NAME             VARCHAR(100),
    COUNTRY                 VARCHAR(100),
    -- Aggregated metrics
    FIRST_ORDER_DATE        DATE,
    LAST_ORDER_DATE         DATE,
    TOTAL_ORDERS            NUMBER,
    TOTAL_REVENUE           NUMBER(14,2),
    TOTAL_PROFIT            NUMBER(14,2),
    AVG_ORDER_VALUE         NUMBER(10,2),
    DAYS_SINCE_LAST_ORDER   NUMBER,
    CUSTOMER_TENURE_DAYS    NUMBER,
    -- Derived segments
    RFM_RECENCY_SCORE       NUMBER(1),
    RFM_FREQUENCY_SCORE     NUMBER(1),
    RFM_MONETARY_SCORE      NUMBER(1),
    CUSTOMER_TIER           VARCHAR(20),    -- Platinum|Gold|Silver|Bronze
    CLV_ESTIMATE            NUMBER(14,2),   -- Customer Lifetime Value
    CHURN_RISK              VARCHAR(10),    -- High|Medium|Low
    DW_UPDATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (SEGMENT, COUNTRY)
COMMENT = 'Customer 360 view with RFM scoring and CLV estimates';

-- ─────────────────────────────────────────────────────────────
-- STORED PROC: Build Gold DAILY_SALES_SUMMARY
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE GOLD_DB.SALES_MART.SP_BUILD_DAILY_SALES_SUMMARY()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    -- Full refresh of summary (or incremental by changing WHERE clause)
    DELETE FROM GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY
    WHERE ORDER_DATE_KEY >= TRY_TO_NUMBER(TO_CHAR(DATEADD('day',-3,CURRENT_DATE()),'YYYYMMDD'));

    INSERT INTO GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY (
        ORDER_DATE_KEY, FULL_DATE, REGION_ID, REGION_NAME, COUNTRY,
        SALES_TERRITORY, ORDER_COUNT, UNIQUE_CUSTOMERS, TOTAL_REVENUE,
        TOTAL_DISCOUNT, NET_REVENUE, TOTAL_PROFIT, AVG_ORDER_VALUE, PROFIT_MARGIN_PCT
    )
    SELECT
        f.ORDER_DATE_KEY,
        d.FULL_DATE,
        r.REGION_ID,
        r.REGION_NAME,
        r.COUNTRY,
        r.SALES_TERRITORY,
        COUNT(DISTINCT f.ORDER_ID)                      AS ORDER_COUNT,
        COUNT(DISTINCT f.SK_CUSTOMER)                   AS UNIQUE_CUSTOMERS,
        SUM(f.TOTAL_AMOUNT)                             AS TOTAL_REVENUE,
        SUM(f.DISCOUNT_AMOUNT)                          AS TOTAL_DISCOUNT,
        SUM(f.NET_AMOUNT)                               AS NET_REVENUE,
        SUM(f.PROFIT)                                   AS TOTAL_PROFIT,
        AVG(f.TOTAL_AMOUNT)                             AS AVG_ORDER_VALUE,
        CASE WHEN SUM(f.NET_AMOUNT) > 0
             THEN (SUM(f.PROFIT) / SUM(f.NET_AMOUNT)) * 100
             ELSE 0 END                                 AS PROFIT_MARGIN_PCT
    FROM SILVER_DB.FACTS.FACT_ORDERS f
    JOIN SILVER_DB.DIMENSIONS.DIM_DATE d  ON d.DATE_KEY  = f.ORDER_DATE_KEY
    LEFT JOIN SILVER_DB.DIMENSIONS.DIM_REGIONS r ON r.SK_REGION = f.SK_REGION
    WHERE f.ORDER_DATE_KEY >= TRY_TO_NUMBER(TO_CHAR(DATEADD('day',-3,CURRENT_DATE()),'YYYYMMDD'))
    GROUP BY 1,2,3,4,5,6;

    RETURN OBJECT_CONSTRUCT('procedure','SP_BUILD_DAILY_SALES_SUMMARY','status','SUCCESS',
        'duration_sec',DATEDIFF('second',v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- STORED PROC: Build Customer 360 with RFM scoring
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE GOLD_DB.CUSTOMER_MART.SP_BUILD_CUSTOMER_360()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    TRUNCATE TABLE GOLD_DB.CUSTOMER_MART.CUSTOMER_360;

    INSERT INTO GOLD_DB.CUSTOMER_MART.CUSTOMER_360 (
        SK_CUSTOMER, CUSTOMER_ID, CUSTOMER_NAME, EMAIL, SEGMENT,
        REGION_NAME, COUNTRY, FIRST_ORDER_DATE, LAST_ORDER_DATE,
        TOTAL_ORDERS, TOTAL_REVENUE, TOTAL_PROFIT, AVG_ORDER_VALUE,
        DAYS_SINCE_LAST_ORDER, CUSTOMER_TENURE_DAYS,
        RFM_RECENCY_SCORE, RFM_FREQUENCY_SCORE, RFM_MONETARY_SCORE,
        CUSTOMER_TIER, CLV_ESTIMATE, CHURN_RISK
    )
    WITH ORDER_METRICS AS (
        SELECT
            f.SK_CUSTOMER,
            MIN(d.FULL_DATE)                        AS FIRST_ORDER_DATE,
            MAX(d.FULL_DATE)                        AS LAST_ORDER_DATE,
            COUNT(DISTINCT f.ORDER_ID)              AS TOTAL_ORDERS,
            SUM(f.TOTAL_AMOUNT)                     AS TOTAL_REVENUE,
            SUM(f.PROFIT)                           AS TOTAL_PROFIT,
            AVG(f.TOTAL_AMOUNT)                     AS AVG_ORDER_VALUE,
            DATEDIFF('day', MAX(d.FULL_DATE), CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER
        FROM SILVER_DB.FACTS.FACT_ORDERS f
        JOIN SILVER_DB.DIMENSIONS.DIM_DATE d ON d.DATE_KEY = f.ORDER_DATE_KEY
        GROUP BY 1
    ),
    RFM_SCORES AS (
        SELECT *,
            NTILE(5) OVER (ORDER BY DAYS_SINCE_LAST_ORDER ASC)  AS RFM_RECENCY,
            NTILE(5) OVER (ORDER BY TOTAL_ORDERS DESC)          AS RFM_FREQUENCY,
            NTILE(5) OVER (ORDER BY TOTAL_REVENUE DESC)         AS RFM_MONETARY
        FROM ORDER_METRICS
    )
    SELECT
        c.SK_CUSTOMER,
        c.CUSTOMER_ID,
        c.CUSTOMER_NAME,
        c.EMAIL,
        c.SEGMENT,
        r.REGION_NAME,
        r.COUNTRY,
        m.FIRST_ORDER_DATE,
        m.LAST_ORDER_DATE,
        m.TOTAL_ORDERS,
        m.TOTAL_REVENUE,
        m.TOTAL_PROFIT,
        m.AVG_ORDER_VALUE,
        m.DAYS_SINCE_LAST_ORDER,
        DATEDIFF('day', c.EFF_START_DATE, CURRENT_DATE()) AS CUSTOMER_TENURE_DAYS,
        m.RFM_RECENCY     AS RFM_RECENCY_SCORE,
        m.RFM_FREQUENCY   AS RFM_FREQUENCY_SCORE,
        m.RFM_MONETARY    AS RFM_MONETARY_SCORE,
        -- Customer tier based on RFM total score
        CASE
            WHEN (m.RFM_RECENCY + m.RFM_FREQUENCY + m.RFM_MONETARY) >= 12 THEN 'Platinum'
            WHEN (m.RFM_RECENCY + m.RFM_FREQUENCY + m.RFM_MONETARY) >= 9  THEN 'Gold'
            WHEN (m.RFM_RECENCY + m.RFM_FREQUENCY + m.RFM_MONETARY) >= 6  THEN 'Silver'
            ELSE 'Bronze'
        END AS CUSTOMER_TIER,
        -- Estimated CLV = avg order value * predicted annual frequency * estimated years
        m.AVG_ORDER_VALUE * (m.TOTAL_ORDERS / NULLIF(DATEDIFF('year',m.FIRST_ORDER_DATE,CURRENT_DATE()),0)) * 3 AS CLV_ESTIMATE,
        -- Churn risk based on recency
        CASE
            WHEN m.DAYS_SINCE_LAST_ORDER > 365 THEN 'High'
            WHEN m.DAYS_SINCE_LAST_ORDER > 180 THEN 'Medium'
            ELSE 'Low'
        END AS CHURN_RISK
    FROM RFM_SCORES m
    JOIN SILVER_DB.DIMENSIONS.DIM_CUSTOMERS c ON c.SK_CUSTOMER = m.SK_CUSTOMER AND c.IS_CURRENT = TRUE
    LEFT JOIN SILVER_DB.DIMENSIONS.DIM_REGIONS r ON r.REGION_ID = c.REGION_ID;

    RETURN OBJECT_CONSTRUCT('procedure','SP_BUILD_CUSTOMER_360','status','SUCCESS',
        'duration_sec',DATEDIFF('second',v_start,CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM);
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- MATERIALIZED VIEW — Fast daily KPIs for BI tools
-- ─────────────────────────────────────────────────────────────
USE SCHEMA REPORTING;

CREATE MATERIALIZED VIEW IF NOT EXISTS MV_SALES_KPI_DAILY AS
SELECT
    d.FULL_DATE,
    d.YEAR,
    d.MONTH_NUM,
    d.MONTH_NAME,
    d.QUARTER_NAME,
    d.IS_WEEKEND,
    r.REGION_NAME,
    r.COUNTRY,
    r.SALES_TERRITORY,
    COUNT(DISTINCT f.ORDER_ID)           AS DAILY_ORDERS,
    COUNT(DISTINCT f.SK_CUSTOMER)        AS ACTIVE_CUSTOMERS,
    SUM(f.TOTAL_AMOUNT)                  AS GROSS_REVENUE,
    SUM(f.NET_AMOUNT)                    AS NET_REVENUE,
    SUM(f.PROFIT)                        AS PROFIT,
    AVG(f.TOTAL_AMOUNT)                  AS AVG_ORDER_VALUE,
    SUM(f.TOTAL_AMOUNT) / NULLIF(COUNT(DISTINCT f.SK_CUSTOMER),0) AS REVENUE_PER_CUSTOMER
FROM SILVER_DB.FACTS.FACT_ORDERS f
JOIN SILVER_DB.DIMENSIONS.DIM_DATE    d ON d.DATE_KEY  = f.ORDER_DATE_KEY
LEFT JOIN SILVER_DB.DIMENSIONS.DIM_REGIONS r ON r.SK_REGION = f.SK_REGION
GROUP BY 1,2,3,4,5,6,7,8,9
COMMENT = 'Materialized view for daily sales KPIs — auto-refreshed';

-- SECURE VIEW for BI tools (masks PII, hides business logic)
CREATE SECURE VIEW IF NOT EXISTS V_SALES_DASHBOARD AS
SELECT
    FULL_DATE,
    YEAR,
    MONTH_NAME,
    QUARTER_NAME,
    REGION_NAME,
    COUNTRY,
    DAILY_ORDERS,
    NET_REVENUE,
    PROFIT,
    AVG_ORDER_VALUE,
    REVENUE_PER_CUSTOMER
FROM GOLD_DB.REPORTING.MV_SALES_KPI_DAILY
ORDER BY FULL_DATE DESC
COMMENT = 'Secure view for BI tool access — no PII, no internal metrics';

-- Running sales trend with window functions
CREATE SECURE VIEW IF NOT EXISTS V_SALES_TREND AS
SELECT
    FULL_DATE,
    REGION_NAME,
    NET_REVENUE,
    SUM(NET_REVENUE) OVER (
        PARTITION BY REGION_NAME
        ORDER BY FULL_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                                               AS ROLLING_7D_REVENUE,
    AVG(NET_REVENUE) OVER (
        PARTITION BY REGION_NAME
        ORDER BY FULL_DATE
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                                               AS ROLLING_30D_AVG,
    NET_REVENUE - LAG(NET_REVENUE) OVER (
        PARTITION BY REGION_NAME ORDER BY FULL_DATE
    )                                                               AS DAY_OVER_DAY_CHANGE,
    RANK() OVER (PARTITION BY FULL_DATE ORDER BY NET_REVENUE DESC)  AS DAILY_REGION_RANK
FROM GOLD_DB.REPORTING.MV_SALES_KPI_DAILY
COMMENT = 'Sales trend analysis with rolling windows and ranking';
