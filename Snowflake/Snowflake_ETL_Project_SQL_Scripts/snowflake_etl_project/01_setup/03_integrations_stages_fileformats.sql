-- ============================================================
-- FILE: 01_setup/03_integrations_stages_fileformats.sql
-- PURPOSE: Storage integration, file formats, external stages
-- RUN AS: ACCOUNTADMIN (for integration), SYSADMIN (for stages)
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- STEP 1: STORAGE INTEGRATION (AWS S3)
-- No hardcoded credentials — uses IAM role trust relationship
-- ─────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_RETAIL_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_ALLOWED_LOCATIONS = (
        's3://retail-analytics-bucket/raw/',
        's3://retail-analytics-bucket/archive/'
    )
    COMMENT = 'Integration for retail analytics S3 bucket';

-- IMPORTANT: Run DESC to get IAM ARN for AWS trust policy setup
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Then in AWS IAM: add these to the S3 bucket trust policy
DESC INTEGRATION S3_RETAIL_INTEGRATION;

/*
  AWS TRUST POLICY (add to your IAM role in AWS):
  {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN from DESC above>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from DESC above>"
        }
      }
    }]
  }

  AWS S3 BUCKET POLICY (read access for Snowflake):
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": ["s3:GetObject","s3:GetObjectVersion"],
        "Resource": "arn:aws:s3:::retail-analytics-bucket/raw/*"
      },
      {
        "Effect": "Allow",
        "Action": ["s3:ListBucket","s3:GetBucketLocation"],
        "Resource": "arn:aws:s3:::retail-analytics-bucket",
        "Condition": {"StringLike": {"s3:prefix": ["raw/*","archive/*"]}}
      }
    ]
  }
*/

-- Grant usage of integration to ETL role
GRANT USAGE ON INTEGRATION S3_RETAIL_INTEGRATION TO ROLE ETL_ROLE;
GRANT USAGE ON INTEGRATION S3_RETAIL_INTEGRATION TO ROLE DATA_ENGINEER_ROLE;

-- ─────────────────────────────────────────────────────────────
-- STEP 2: FILE FORMATS
-- ─────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;
USE DATABASE COMMON_DB;
USE SCHEMA INGESTION;

-- CSV format for orders, customers, products
CREATE FILE FORMAT IF NOT EXISTS CSV_STANDARD_FMT
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = ','
    RECORD_DELIMITER             = '\n'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', 'N/A', '\\N', '')
    EMPTY_FIELD_AS_NULL          = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE                       = '\\'
    TRIM_SPACE                   = TRUE
    DATE_FORMAT                  = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT             = 'YYYY-MM-DD HH24:MI:SS'
    COMPRESSION                  = AUTO
    COMMENT                      = 'Standard CSV format with header, auto-compression';

-- CSV with pipe delimiter (legacy source system format)
CREATE FILE FORMAT IF NOT EXISTS CSV_PIPE_FMT
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = '|'
    RECORD_DELIMITER             = '\n'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL','')
    EMPTY_FIELD_AS_NULL          = TRUE
    COMPRESSION                  = AUTO
    COMMENT                      = 'Pipe-delimited CSV for legacy feeds';

-- JSON format for event data
CREATE FILE FORMAT IF NOT EXISTS JSON_FMT
    TYPE              = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = FALSE
    COMPRESSION       = AUTO
    COMMENT           = 'JSON array format with null retention';

-- Parquet format for bulk historical loads
CREATE FILE FORMAT IF NOT EXISTS PARQUET_FMT
    TYPE               = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE
    COMMENT            = 'Parquet with Snappy compression for historical loads';

-- ─────────────────────────────────────────────────────────────
-- STEP 3: EXTERNAL STAGES (pointing to S3)
-- ─────────────────────────────────────────────────────────────

-- Orders stage
CREATE STAGE IF NOT EXISTS STG_S3_ORDERS
    URL                 = 's3://retail-analytics-bucket/raw/orders/'
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    FILE_FORMAT         = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    COMMENT             = 'S3 stage for daily orders CSV files';

-- Customers stage
CREATE STAGE IF NOT EXISTS STG_S3_CUSTOMERS
    URL                 = 's3://retail-analytics-bucket/raw/customers/'
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    FILE_FORMAT         = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    COMMENT             = 'S3 stage for customer master CSV files';

-- Products stage
CREATE STAGE IF NOT EXISTS STG_S3_PRODUCTS
    URL                 = 's3://retail-analytics-bucket/raw/products/'
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    FILE_FORMAT         = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    COMMENT             = 'S3 stage for product catalog CSV files';

-- Order items stage
CREATE STAGE IF NOT EXISTS STG_S3_ORDER_ITEMS
    URL                 = 's3://retail-analytics-bucket/raw/order_items/'
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    FILE_FORMAT         = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    COMMENT             = 'S3 stage for order line items CSV files';

-- Regions stage
CREATE STAGE IF NOT EXISTS STG_S3_REGIONS
    URL                 = 's3://retail-analytics-bucket/raw/regions/'
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    FILE_FORMAT         = (FORMAT_NAME = 'COMMON_DB.INGESTION.CSV_STANDARD_FMT')
    COMMENT             = 'S3 stage for region reference CSV files';

-- Archive stage (for processed files)
CREATE STAGE IF NOT EXISTS STG_S3_ARCHIVE
    URL                 = 's3://retail-analytics-bucket/archive/'
    STORAGE_INTEGRATION = S3_RETAIL_INTEGRATION
    COMMENT             = 'S3 archive stage for processed files';

-- Verify stages
LIST @STG_S3_ORDERS;
SHOW STAGES IN SCHEMA COMMON_DB.INGESTION;

-- ─────────────────────────────────────────────────────────────
-- STEP 4: SEQUENCES for surrogate keys
-- ─────────────────────────────────────────────────────────────

USE SCHEMA COMMON_DB.UTILITIES;

CREATE SEQUENCE IF NOT EXISTS SEQ_CUSTOMER_SK   START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_PRODUCT_SK    START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_REGION_SK     START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS SEQ_ORDER_SK      START = 1 INCREMENT = 1;

-- ─────────────────────────────────────────────────────────────
-- STEP 5: NOTIFICATION INTEGRATION (AWS SNS for task alerts)
-- ─────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS SNS_PIPELINE_ALERTS
    TYPE                  = QUEUE
    NOTIFICATION_PROVIDER = AWS_SNS
    ENABLED               = TRUE
    AWS_SNS_TOPIC_ARN     = 'arn:aws:sns:us-east-1:123456789012:retail-pipeline-alerts'
    AWS_SNS_ROLE_ARN      = 'arn:aws:iam::123456789012:role/SnowflakeSNSRole'
    COMMENT               = 'SNS integration for pipeline failure alerts';

-- Get IAM details for SNS trust policy setup
DESC INTEGRATION SNS_PIPELINE_ALERTS;

GRANT USAGE ON INTEGRATION SNS_PIPELINE_ALERTS TO ROLE ETL_ROLE;
GRANT USAGE ON INTEGRATION SNS_PIPELINE_ALERTS TO ROLE DATA_ENGINEER_ROLE;
