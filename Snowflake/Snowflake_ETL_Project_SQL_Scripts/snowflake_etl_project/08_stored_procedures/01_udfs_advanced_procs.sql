-- ============================================================
-- FILE: 08_stored_procedures/01_udfs_advanced_procs.sql
-- PURPOSE: UDFs, Python stored procedures, utility functions
-- RUN AS: DATA_ENGINEER_ROLE
-- ============================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE COMMON_DB;
USE SCHEMA UTILITIES;

-- ─────────────────────────────────────────────────────────────
-- SECTION A: SCALAR UDFs (SQL)
-- ─────────────────────────────────────────────────────────────

-- Clean and standardize phone numbers
CREATE OR REPLACE FUNCTION UDF_CLEAN_PHONE(phone_raw VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Standardize phone to +1-XXX-XXX-XXXX format'
AS $$
    CASE
        WHEN phone_raw IS NULL THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(phone_raw, '[^0-9]', '')) = 10
            THEN '+1-' ||
                 SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''),1,3) || '-' ||
                 SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''),4,3) || '-' ||
                 SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''),7,4)
        WHEN LENGTH(REGEXP_REPLACE(phone_raw, '[^0-9]', '')) = 11
            THEN '+' ||
                 SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''),1,1) || '-' ||
                 SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''),2,3) || '-' ||
                 SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''),5,3) || '-' ||
                 SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''),8,4)
        ELSE phone_raw  -- return as-is if unknown format
    END
$$;

-- Classify order value tier
CREATE OR REPLACE FUNCTION UDF_ORDER_TIER(amount NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Classify order into revenue tier'
AS $$
    CASE
        WHEN amount IS NULL   THEN 'Unknown'
        WHEN amount <   50    THEN 'Micro'
        WHEN amount <   250   THEN 'Small'
        WHEN amount <   1000  THEN 'Medium'
        WHEN amount <   5000  THEN 'Large'
        ELSE                       'Enterprise'
    END
$$;

-- Calculate business days between two dates
CREATE OR REPLACE FUNCTION UDF_BUSINESS_DAYS(start_date DATE, end_date DATE)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Count business days (Mon-Fri) between two dates'
AS $$
    (DATEDIFF('day', start_date, end_date) + 1)
    - FLOOR(DATEDIFF('week', start_date, end_date)) * 2
    - CASE WHEN DAYOFWEEK(start_date) = 1 THEN 1 ELSE 0 END
    - CASE WHEN DAYOFWEEK(end_date)   = 7 THEN 1 ELSE 0 END
$$;

-- Fiscal year from calendar date (Apr-Mar fiscal year)
CREATE OR REPLACE FUNCTION UDF_FISCAL_YEAR(d DATE)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Returns fiscal year string, e.g. FY2024 for Apr2023-Mar2024'
AS $$
    'FY' || CASE WHEN MONTH(d) >= 4 THEN YEAR(d)+1 ELSE YEAR(d) END
$$;

-- Data quality hash (for change detection without updated_at)
CREATE OR REPLACE FUNCTION UDF_ROW_HASH(col1 VARCHAR, col2 VARCHAR,
                                         col3 VARCHAR, col4 VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generate consistent hash of key columns for change detection'
AS $$
    MD5(CONCAT(
        COALESCE(col1,'__NULL__'), '|',
        COALESCE(col2,'__NULL__'), '|',
        COALESCE(col3,'__NULL__'), '|',
        COALESCE(col4,'__NULL__')
    ))
$$;

-- ─────────────────────────────────────────────────────────────
-- SECTION B: VECTORIZED PYTHON UDF (batch processing)
-- ─────────────────────────────────────────────────────────────

-- Customer segment classifier using pandas
CREATE OR REPLACE FUNCTION UDF_CLASSIFY_CUSTOMER_SEGMENT(
    total_orders FLOAT,
    avg_order_value FLOAT,
    days_since_last_order FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('pandas')
HANDLER = 'compute'
COMMENT = 'Vectorized Python UDF — classifies customer segment from metrics'
AS $$
import pandas as pd

def compute(orders: pd.Series, avg_val: pd.Series, days_since: pd.Series) -> pd.Series:
    score = pd.Series(0.0, index=orders.index)
    # Order frequency score (0-3)
    score += (orders >= 10).astype(float) * 3
    score += ((orders >= 5) & (orders < 10)).astype(float) * 2
    score += ((orders >= 2) & (orders < 5)).astype(float) * 1
    # Value score (0-3)
    score += (avg_val >= 1000).astype(float) * 3
    score += ((avg_val >= 500) & (avg_val < 1000)).astype(float) * 2
    score += ((avg_val >= 100) & (avg_val < 500)).astype(float) * 1
    # Recency penalty
    score -= (days_since > 365).astype(float) * 2
    score -= ((days_since > 180) & (days_since <= 365)).astype(float) * 1
    return pd.cut(score, bins=[-999, 1, 3, 5, 999],
                  labels=['At-Risk', 'Standard', 'Loyal', 'Champion'])
$$;

-- ─────────────────────────────────────────────────────────────
-- SECTION C: UDTF — Table function (returns multiple rows)
-- ─────────────────────────────────────────────────────────────

-- Generate date series between two dates
CREATE OR REPLACE FUNCTION UDF_DATE_SERIES(start_date DATE, end_date DATE)
RETURNS TABLE (date_value DATE)
LANGUAGE SQL
COMMENT = 'Returns one row per calendar day between start and end date'
AS $$
    SELECT DATEADD('day', SEQ4(), start_date)::DATE AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT =>
        GREATEST(DATEDIFF('day', start_date, end_date) + 1, 0)
    ))
    WHERE DATEADD('day', SEQ4(), start_date) <= end_date
$$;

-- Usage: fill gaps in daily sales data
-- SELECT * FROM TABLE(UDF_DATE_SERIES('2024-01-01'::DATE, '2024-01-31'::DATE));

-- Split comma-separated product categories into rows
CREATE OR REPLACE FUNCTION UDF_SPLIT_TAGS(tag_string VARCHAR, delimiter VARCHAR)
RETURNS TABLE (tag_value VARCHAR, tag_position NUMBER)
LANGUAGE SQL
COMMENT = 'Splits delimited string into individual rows with position'
AS $$
    SELECT TRIM(VALUE)::VARCHAR, INDEX + 1
    FROM TABLE(SPLIT_TO_TABLE(tag_string, delimiter))
    WHERE TRIM(VALUE) != ''
$$;

-- ─────────────────────────────────────────────────────────────
-- SECTION D: PYTHON STORED PROCEDURE — Data profiling
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE COMMON_DB.UTILITIES.SP_PROFILE_TABLE(
    db_name     VARCHAR,
    schema_name VARCHAR,
    table_name  VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'run'
EXECUTE AS OWNER
COMMENT = 'Profile a table: row count, null counts, distinct counts per column'
AS $$
import pandas as pd
import json

def run(session, db_name: str, schema_name: str, table_name: str):
    full_name = f"{db_name}.{schema_name}.{table_name}"
    try:
        df = session.table(full_name).to_pandas()
        profile = {
            "table": full_name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {}
        }
        for col in df.columns:
            col_stats = {
                "dtype":          str(df[col].dtype),
                "null_count":     int(df[col].isna().sum()),
                "null_pct":       round(df[col].isna().sum() / len(df) * 100, 2) if len(df) > 0 else 0,
                "distinct_count": int(df[col].nunique()),
                "distinct_pct":   round(df[col].nunique() / len(df) * 100, 2) if len(df) > 0 else 0
            }
            if df[col].dtype in ['int64', 'float64']:
                col_stats["min"]  = float(df[col].min()) if df[col].notna().any() else None
                col_stats["max"]  = float(df[col].max()) if df[col].notna().any() else None
                col_stats["mean"] = float(df[col].mean()) if df[col].notna().any() else None
            profile["columns"][col] = col_stats
        return profile
    except Exception as e:
        return {"error": str(e), "table": full_name}
$$;

-- Usage:
-- CALL COMMON_DB.UTILITIES.SP_PROFILE_TABLE('BRONZE_DB', 'RAW', 'RAW_ORDERS');

-- ─────────────────────────────────────────────────────────────
-- SECTION E: JAVASCRIPT STORED PROCEDURE — Dynamic DDL
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE COMMON_DB.UTILITIES.SP_ARCHIVE_OLD_DATA(
    target_table    VARCHAR,   -- e.g. 'BRONZE_DB.RAW.RAW_ORDERS'
    date_column     VARCHAR,   -- e.g. 'ORDER_DATE'
    retention_days  NUMBER     -- e.g. 90
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
COMMENT = 'Archive rows older than retention_days to an archive table'
AS $$
    var result = { procedure: 'SP_ARCHIVE_OLD_DATA', rows_archived: 0 };
    try {
        var parts   = TARGET_TABLE.split('.');
        var db      = parts[0], schema = parts[1], tbl = parts[2];
        var archive = `${db}.ARCHIVE.${tbl}_ARCHIVE`;

        // Create archive table if not exists (clone source structure)
        var chk = snowflake.execute({
            sqlText: `SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLES
                      WHERE TABLE_CATALOG = '${db}'
                        AND TABLE_SCHEMA  = 'ARCHIVE'
                        AND TABLE_NAME    = '${tbl}_ARCHIVE'`
        });
        chk.next();
        if (chk.getColumnValue('CNT') === 0) {
            snowflake.execute({
                sqlText: `CREATE TABLE ${archive} CLONE ${TARGET_TABLE}`
            });
        }

        // Move old rows to archive
        var move = snowflake.execute({
            sqlText: `INSERT INTO ${archive}
                      SELECT * FROM ${TARGET_TABLE}
                      WHERE ${DATE_COLUMN} < DATEADD('day', -${RETENTION_DAYS}, CURRENT_DATE())`
        });
        move.next();
        result.rows_archived = move.getColumnValue(1) || 0;

        // Delete from source
        snowflake.execute({
            sqlText: `DELETE FROM ${TARGET_TABLE}
                      WHERE ${DATE_COLUMN} < DATEADD('day', -${RETENTION_DAYS}, CURRENT_DATE())`
        });

        result.status = 'SUCCESS';
        result.archive_table = archive;
    } catch(e) {
        result.status = 'FAILED';
        result.error  = e.message;
    }
    return result;
$$;

-- ─────────────────────────────────────────────────────────────
-- SECTION F: TIME TRAVEL & ZERO-COPY CLONE utilities
-- ─────────────────────────────────────────────────────────────

-- Procedure: create a named snapshot of any table
CREATE OR REPLACE PROCEDURE COMMON_DB.UTILITIES.SP_CREATE_SNAPSHOT(
    source_table    VARCHAR,
    snapshot_suffix VARCHAR   -- e.g. 'PRE_MIGRATION_20240315'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    snap_name VARCHAR;
    sql_stmt  VARCHAR;
BEGIN
    -- Parse and build snapshot table name
    snap_name := source_table || '_SNAP_' || UPPER(:snapshot_suffix);

    -- Create zero-copy clone
    sql_stmt := 'CREATE OR REPLACE TABLE ' || snap_name || ' CLONE ' || :source_table;
    EXECUTE IMMEDIATE sql_stmt;

    RETURN 'Snapshot created: ' || snap_name;
END;
$$;

-- Usage before risky migrations:
-- CALL COMMON_DB.UTILITIES.SP_CREATE_SNAPSHOT(
--     'SILVER_DB.DIMENSIONS.DIM_CUSTOMERS', 'PRE_MIGRATION_20240315'
-- );

-- ─────────────────────────────────────────────────────────────
-- TEST UDFS
-- ─────────────────────────────────────────────────────────────

SELECT UDF_CLEAN_PHONE('555.867.5309')           AS cleaned_phone;
SELECT UDF_CLEAN_PHONE('(555) 867-5309')         AS cleaned_phone2;
SELECT UDF_ORDER_TIER(1500)                      AS tier;
SELECT UDF_BUSINESS_DAYS('2024-03-11', '2024-03-15') AS biz_days; -- 3 days
SELECT UDF_FISCAL_YEAR('2024-05-15'::DATE)       AS fiscal_year;  -- FY2025

SELECT * FROM TABLE(UDF_DATE_SERIES('2024-01-29'::DATE, '2024-02-03'::DATE));
