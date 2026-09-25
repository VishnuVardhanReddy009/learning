-- ============================================================
-- FILE: 01_setup/01_databases_warehouses.sql
-- PURPOSE: Create all databases, schemas, and virtual warehouses
-- RUN AS: SYSADMIN
-- ============================================================

USE ROLE SYSADMIN;

-- ─────────────────────────────────────────────────────────────
-- DATABASES — Medallion Architecture
-- ─────────────────────────────────────────────────────────────

-- BRONZE: Raw landing zone — exact copy of source, no transforms
CREATE DATABASE IF NOT EXISTS BRONZE_DB
    DATA_RETENTION_TIME_IN_DAYS = 1    -- transient-like: can recreate from S3
    COMMENT = 'Raw landing zone — unmodified source data from S3';

-- SILVER: Cleansed, typed, validated, conformed
CREATE DATABASE IF NOT EXISTS SILVER_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Cleansed and validated data — dimensions and facts';

-- GOLD: Business-ready aggregations and data marts
CREATE DATABASE IF NOT EXISTS GOLD_DB
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'Business aggregations, KPIs, data marts for BI';

-- COMMON: Shared objects — file formats, stages, sequences, utilities
CREATE DATABASE IF NOT EXISTS COMMON_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Shared objects: file formats, stages, sequences, utilities';

-- ─────────────────────────────────────────────────────────────
-- SCHEMAS
-- ─────────────────────────────────────────────────────────────

-- Bronze schemas
CREATE SCHEMA IF NOT EXISTS BRONZE_DB.RAW
    COMMENT = 'Raw tables loaded directly from S3 via COPY INTO';

CREATE SCHEMA IF NOT EXISTS BRONZE_DB.ARCHIVE
    COMMENT = 'Archived raw files and load history';

-- Silver schemas
CREATE SCHEMA IF NOT EXISTS SILVER_DB.DIMENSIONS
    COMMENT = 'Conformed SCD2 dimension tables';

CREATE SCHEMA IF NOT EXISTS SILVER_DB.FACTS
    COMMENT = 'Cleansed and validated fact tables';

CREATE SCHEMA IF NOT EXISTS SILVER_DB.STAGING
    COMMENT = 'Transient staging area for ELT transforms';

-- Gold schemas
CREATE SCHEMA IF NOT EXISTS GOLD_DB.SALES_MART
    COMMENT = 'Sales analytics data mart';

CREATE SCHEMA IF NOT EXISTS GOLD_DB.CUSTOMER_MART
    COMMENT = 'Customer analytics data mart';

CREATE SCHEMA IF NOT EXISTS GOLD_DB.PRODUCT_MART
    COMMENT = 'Product performance data mart';

CREATE SCHEMA IF NOT EXISTS GOLD_DB.REPORTING
    COMMENT = 'Reporting views for BI tools';

-- Common schemas
CREATE SCHEMA IF NOT EXISTS COMMON_DB.INGESTION
    COMMENT = 'Stages, file formats, pipes';

CREATE SCHEMA IF NOT EXISTS COMMON_DB.UTILITIES
    COMMENT = 'UDFs, sequences, helper objects';

CREATE SCHEMA IF NOT EXISTS COMMON_DB.AUDIT
    COMMENT = 'Pipeline audit logs and data quality results';

-- ─────────────────────────────────────────────────────────────
-- VIRTUAL WAREHOUSES — Separated by workload type
-- ─────────────────────────────────────────────────────────────

-- INGESTION warehouse: COPY INTO, Snowpipe support loads
-- XS is sufficient — COPY INTO is I/O bound, not compute bound
CREATE WAREHOUSE IF NOT EXISTS INGESTION_WH
    WAREHOUSE_SIZE    = 'XSMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    WAREHOUSE_TYPE    = 'STANDARD'
    COMMENT           = 'Used for COPY INTO and data loading operations';

-- TRANSFORM warehouse: Silver and Gold layer ELT processing
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE    = 'MEDIUM'
    AUTO_SUSPEND      = 120
    AUTO_RESUME       = TRUE
    WAREHOUSE_TYPE    = 'STANDARD'
    COMMENT           = 'Used for Silver/Gold layer transformations and streams/tasks';

-- ANALYTICS warehouse: Business users and BI tool queries
-- Multi-cluster for concurrency
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE    = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY    = 'STANDARD'
    AUTO_SUSPEND      = 180
    AUTO_RESUME       = TRUE
    WAREHOUSE_TYPE    = 'STANDARD'
    COMMENT           = 'Multi-cluster for concurrent BI queries';

-- DATA_SCIENCE warehouse: Ad-hoc heavy analysis and ML
CREATE WAREHOUSE IF NOT EXISTS DATA_SCIENCE_WH
    WAREHOUSE_SIZE    = 'LARGE'
    WAREHOUSE_TYPE    = 'SNOWPARK-OPTIMIZED'   -- 16x memory for Python/ML
    AUTO_SUSPEND      = 300
    AUTO_RESUME       = TRUE
    COMMENT           = 'Snowpark-optimized for ML and heavy Python workloads';

-- DEV warehouse: Developer sandbox — aggressively suspended
CREATE WAREHOUSE IF NOT EXISTS DEV_WH
    WAREHOUSE_SIZE    = 'XSMALL'
    AUTO_SUSPEND      = 30
    AUTO_RESUME       = TRUE
    COMMENT           = 'Developer sandbox — aggressive auto-suspend';

-- Verify
SHOW WAREHOUSES;
SHOW DATABASES;
