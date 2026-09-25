-- ============================================================
-- FILE: 06_security/01_security_policies.sql
-- PURPOSE: Dynamic masking, row access, network policies
-- RUN AS: SECURITYADMIN (policies) + SYSADMIN (apply to objects)
-- ============================================================

USE ROLE SECURITYADMIN;
USE DATABASE SILVER_DB;

-- ─────────────────────────────────────────────────────────────
-- SECTION A: DYNAMIC DATA MASKING POLICIES
-- ─────────────────────────────────────────────────────────────

-- Email masking: full for ETL/admin, partial for analysts, masked for others
CREATE OR REPLACE MASKING POLICY SILVER_DB.DIMENSIONS.MASK_EMAIL
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE','ETL_ADMIN_ROLE','ACCOUNTADMIN')
            THEN val
        WHEN CURRENT_ROLE() = 'SENIOR_ANALYST_ROLE'
            THEN REGEXP_REPLACE(val, '(^[^@]{1,2})([^@]*)(@.*)', '\\1***\\3')
        ELSE
            CONCAT('***@', SPLIT_PART(val,'@',2))
    END
COMMENT = 'Email masking: full/partial/domain-only based on role';

-- Phone masking: show last 4 digits only
CREATE OR REPLACE MASKING POLICY SILVER_DB.DIMENSIONS.MASK_PHONE
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE','ETL_ADMIN_ROLE','ACCOUNTADMIN')
            THEN val
        ELSE
            CONCAT('***-***-', RIGHT(REGEXP_REPLACE(val,'[^0-9]',''), 4))
    END
COMMENT = 'Phone masking: last 4 digits only for non-engineers';

-- Credit limit masking: null for analysts below senior
CREATE OR REPLACE MASKING POLICY SILVER_DB.DIMENSIONS.MASK_CREDIT_LIMIT
AS (val NUMBER) RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE','ETL_ADMIN_ROLE','ACCOUNTADMIN')
            THEN val
        WHEN CURRENT_ROLE() = 'SENIOR_ANALYST_ROLE'
            THEN ROUND(val, -3)     -- round to nearest 1000
        ELSE
            NULL
    END
COMMENT = 'Credit limit masking: null for junior/standard analysts';

-- Customer name masking: anonymize for junior analysts
CREATE OR REPLACE MASKING POLICY SILVER_DB.DIMENSIONS.MASK_CUSTOMER_NAME
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE','ETL_ADMIN_ROLE',
                                 'ACCOUNTADMIN','SENIOR_ANALYST_ROLE')
            THEN val
        ELSE
            CONCAT('Customer-', MD5(val))   -- pseudonymized, consistent
    END
COMMENT = 'Customer name masking: pseudonymized for junior roles';

-- ─────────────────────────────────────────────────────────────
-- APPLY MASKING POLICIES TO COLUMNS
-- ─────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;

ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN EMAIL         SET MASKING POLICY SILVER_DB.DIMENSIONS.MASK_EMAIL;

ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN PHONE         SET MASKING POLICY SILVER_DB.DIMENSIONS.MASK_PHONE;

ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN CREDIT_LIMIT  SET MASKING POLICY SILVER_DB.DIMENSIONS.MASK_CREDIT_LIMIT;

ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN CUSTOMER_NAME SET MASKING POLICY SILVER_DB.DIMENSIONS.MASK_CUSTOMER_NAME;

-- Also apply to Gold customer_360 table
ALTER TABLE GOLD_DB.CUSTOMER_MART.CUSTOMER_360
    MODIFY COLUMN EMAIL         SET MASKING POLICY SILVER_DB.DIMENSIONS.MASK_EMAIL;

-- ─────────────────────────────────────────────────────────────
-- SECTION B: ROW ACCESS POLICIES
-- ─────────────────────────────────────────────────────────────

USE ROLE SECURITYADMIN;

-- Mapping table: user → allowed sales territories
CREATE TABLE IF NOT EXISTS COMMON_DB.UTILITIES.USER_TERRITORY_ACCESS (
    USERNAME            VARCHAR(100),
    SALES_TERRITORY     VARCHAR(100),
    ACCESS_TYPE         VARCHAR(20),    -- READ | ADMIN
    VALID_FROM          DATE DEFAULT CURRENT_DATE(),
    VALID_TO            DATE            -- NULL = still active
);

-- Sample data (in production: managed by HR/IAM system)
INSERT INTO COMMON_DB.UTILITIES.USER_TERRITORY_ACCESS VALUES
    ('BOB_ANALYST',     'North America',  'READ', CURRENT_DATE(), NULL),
    ('BOB_ANALYST',     'Latin America',  'READ', CURRENT_DATE(), NULL),
    ('CAROL_SCIENTIST', 'APAC',           'READ', CURRENT_DATE(), NULL),
    ('CAROL_SCIENTIST', 'EMEA',           'READ', CURRENT_DATE(), NULL);

-- Row access policy: restrict Gold sales data by territory
CREATE OR REPLACE ROW ACCESS POLICY GOLD_DB.SALES_MART.POLICY_TERRITORY_ACCESS
AS (row_territory VARCHAR) RETURNS BOOLEAN ->
    CASE
        -- Engineers and admins see all territories
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE','ETL_ADMIN_ROLE','ACCOUNTADMIN')
            THEN TRUE
        -- Senior analysts see all
        WHEN CURRENT_ROLE() = 'SENIOR_ANALYST_ROLE'
            THEN TRUE
        -- All other roles: check mapping table
        ELSE EXISTS (
            SELECT 1
            FROM COMMON_DB.UTILITIES.USER_TERRITORY_ACCESS
            WHERE USERNAME       = CURRENT_USER()
              AND SALES_TERRITORY = row_territory
              AND VALID_FROM     <= CURRENT_DATE()
              AND (VALID_TO IS NULL OR VALID_TO >= CURRENT_DATE())
        )
    END
COMMENT = 'Row-level access: users see only their assigned territories';

-- Apply to Gold daily summary
ALTER TABLE GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY
    ADD ROW ACCESS POLICY GOLD_DB.SALES_MART.POLICY_TERRITORY_ACCESS ON (SALES_TERRITORY);

-- ─────────────────────────────────────────────────────────────
-- SECTION C: NETWORK POLICIES
-- ─────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

-- Corporate office IP policy (apply to production account)
CREATE NETWORK POLICY IF NOT EXISTS NP_CORPORATE_ACCESS
    ALLOWED_IP_LIST = (
        '203.0.113.0/24',    -- Corporate HQ
        '198.51.100.0/24',   -- Remote access VPN
        '192.0.2.50'         -- Data center egress IP
    )
    BLOCKED_IP_LIST = (
        '198.51.100.99'      -- Compromised IP example
    )
    COMMENT = 'Restrict Snowflake access to corporate IPs and VPN';

-- Stricter policy for ETL service accounts
CREATE NETWORK POLICY IF NOT EXISTS NP_ETL_SERVICE_ONLY
    ALLOWED_IP_LIST = (
        '203.0.113.100',     -- ETL server
        '203.0.113.101'      -- ETL server failover
    )
    COMMENT = 'ETL service account — data center IPs only';

-- Apply to ETL service account (stricter than account policy)
ALTER USER ETL_SERVICE_ACCOUNT SET NETWORK_POLICY = NP_ETL_SERVICE_ONLY;

-- ─────────────────────────────────────────────────────────────
-- SECTION D: OBJECT TAGGING for Data Classification
-- ─────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;

-- Create tags for data classification
CREATE TAG IF NOT EXISTS COMMON_DB.UTILITIES.TAG_PII
    ALLOWED_VALUES = ('email','phone','name','address','financial')
    COMMENT = 'PII classification tag';

CREATE TAG IF NOT EXISTS COMMON_DB.UTILITIES.TAG_SENSITIVITY
    ALLOWED_VALUES = ('public','internal','confidential','restricted')
    COMMENT = 'Data sensitivity classification';

-- Tag PII columns
ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN EMAIL         SET TAG COMMON_DB.UTILITIES.TAG_PII = 'email';
ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN PHONE         SET TAG COMMON_DB.UTILITIES.TAG_PII = 'phone';
ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN CUSTOMER_NAME SET TAG COMMON_DB.UTILITIES.TAG_PII = 'name';
ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    MODIFY COLUMN CREDIT_LIMIT  SET TAG COMMON_DB.UTILITIES.TAG_PII = 'financial';

-- Tag tables by sensitivity
ALTER TABLE SILVER_DB.DIMENSIONS.DIM_CUSTOMERS
    SET TAG COMMON_DB.UTILITIES.TAG_SENSITIVITY = 'confidential';
ALTER TABLE GOLD_DB.SALES_MART.DAILY_SALES_SUMMARY
    SET TAG COMMON_DB.UTILITIES.TAG_SENSITIVITY = 'internal';

-- Query tagged objects
SELECT TAG_NAME, TAG_VALUE, OBJECT_NAME, COLUMN_NAME
FROM TABLE(SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES_WITH_LINEAGE(
    'COMMON_DB.UTILITIES.TAG_PII'
));

-- ─────────────────────────────────────────────────────────────
-- SECTION E: VERIFY SECURITY SETUP
-- ─────────────────────────────────────────────────────────────

-- Test masking as analyst
USE ROLE ANALYST_ROLE;
SELECT CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CREDIT_LIMIT
FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS LIMIT 5;
-- Should see: masked name, masked email, masked phone, NULL credit limit

-- Test masking as engineer
USE ROLE DATA_ENGINEER_ROLE;
SELECT CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CREDIT_LIMIT
FROM SILVER_DB.DIMENSIONS.DIM_CUSTOMERS LIMIT 5;
-- Should see: real values

-- Inspect all policies on DIM_CUSTOMERS
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
WHERE REF_ENTITY_NAME = 'DIM_CUSTOMERS';
