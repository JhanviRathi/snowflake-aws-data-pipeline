-- ============================================================
-- End-to-End Data Pipeline: Snowflake + AWS S3 Integration
-- Dataset: Superstore Sales Data
-- Tools: Snowflake, AWS S3, SQL
-- ============================================================

-- STEP 1: Setup Database and Schema
CREATE DATABASE PIPELINE_DB;
CREATE SCHEMA PIPELINE_DB.RAW;
USE DATABASE PIPELINE_DB;
USE SCHEMA RAW;

-- STEP 2: Create Storage Integration with AWS S3
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::285527663144:role/snowflake_s3_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://pipeline-demo-123');

DESC INTEGRATION s3_int;

-- STEP 3: Create External Stage pointing to S3 bucket
CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://pipeline-demo-123/';

LIST @my_s3_stage;

-- STEP 4: Define CSV File Format
CREATE OR REPLACE FILE FORMAT ff_csv
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- STEP 5: Create Raw Table and Load Data from S3
CREATE OR REPLACE TABLE raw_superstore (
  col1 STRING, col2 STRING, col3 STRING, col4 STRING,
  col5 STRING, col6 STRING, col7 STRING, col8 STRING,
  col9 STRING, col10 STRING, col11 STRING, col12 STRING,
  col13 STRING, col14 STRING, col15 STRING, col16 STRING,
  col17 STRING, col18 STRING
);

COPY INTO raw_superstore
  FROM @my_s3_stage
  FILE_FORMAT = ff_csv
  ON_ERROR = 'CONTINUE';

SELECT COUNT(*) FROM raw_superstore;
SELECT * FROM raw_superstore LIMIT 5;

-- STEP 6: Create Clean Transformed Table
CREATE OR REPLACE TABLE clean_superstore AS
SELECT
  col2  AS order_id,
  col3  AS order_date,
  col4  AS ship_date,
  col6  AS customer_id,
  col7  AS segment,
  col8  AS country,
  col9  AS city,
  col10 AS state,
  col16 AS sales,
  col17 AS profit
FROM raw_superstore;

SELECT * FROM clean_superstore LIMIT 10;

-- STEP 7: Create Stream for Change Data Capture
CREATE OR REPLACE STREAM superstore_stream
  ON TABLE raw_superstore;

-- STEP 8: Create Task for Incremental Processing
CREATE OR REPLACE TASK superstore_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
AS
  INSERT INTO clean_superstore
  SELECT
    col2, col3, col4, col6, col7,
    col8, col9, col10, col16, col17
  FROM superstore_stream;

ALTER TASK superstore_task RESUME;
