# Snowflake End-to-End ETL Project
## Retail Sales Analytics Platform — Medallion Architecture

### Project Overview
This project implements a production-grade ETL pipeline for a retail company
loading CSV sales data from AWS S3 into Snowflake using the Medallion
(Bronze → Silver → Gold) architecture.

### Topics Covered
- Storage Integration (S3 → Snowflake)
- External Stages & File Formats
- COPY INTO with error handling
- Snowpipe (continuous ingestion)
- Streams & Tasks (CDC pipelines)
- SCD Type 2 (dimension tables)
- RBAC & Role hierarchy
- Dynamic Data Masking
- Row Access Policies
- Resource Monitors
- Time Travel & Zero-Copy Cloning
- Stored Procedures (SQL + Python)
- UDFs
- Views (Standard, Secure, Materialized)
- Task DAGs with SNS notifications
- Data Quality checks
- ACCOUNT_USAGE monitoring

### Architecture
```
AWS S3 (CSV files)
    │
    ▼  Storage Integration + External Stage
BRONZE (Raw Layer)
    │  Snowpipe AUTO_INGEST
    │  COPY INTO
    ▼
SILVER (Cleansed Layer)
    │  Streams + Tasks
    │  SCD Type 2 Dimensions
    │  Data Quality
    ▼
GOLD (Business Layer)
    │  Aggregations + Marts
    │  Materialized Views
    ▼
Reporting / BI Tools
```

### Source Data (CSV Files in S3)
- orders.csv          — transactional orders
- customers.csv       — customer master data
- products.csv        — product catalog
- order_items.csv     — line items per order
- regions.csv         — region/geography reference

### Execution Order
1. 01_setup/         — databases, warehouses, roles, integrations
2. 02_bronze/        — raw landing layer
3. 03_silver/        — cleansed dimensions & facts
4. 04_gold/          — business aggregations & marts
5. 05_pipelines/     — streams, tasks, DAGs
6. 06_security/      — masking, row policies, RBAC
7. 07_monitoring/    — resource monitors, alerts, SNS
8. 08_stored_procedures/ — all procs and UDFs
9. 09_data_quality/  — DQ checks and rules
