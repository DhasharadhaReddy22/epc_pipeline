-- create storage integration for accessing S3 bucket
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION epc_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::11111111111:role/dummy' -- some dummy role, will be updated later
  STORAGE_ALLOWED_LOCATIONS = ('s3://epc-snowflake-project/raw/')
  COMMENT = 'Integration for accessing S3 data bucket for epc project';

DESCRIBE INTEGRATION epc_s3_int; -- copy the STORAGE_AWS_EXTERNAL_ID and STORAGE_AWS_IAM_USER_ARN for creation of STORAGE_AWS_ROLE_ARN in AWS

ALTER STORAGE INTEGRATION epc_s3_int SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws_account_id>:role/epc-project-role'; -- paste the arn of the role created for this snowflake account's STORAGE_AWS_IAM_USER_ARN to take access to S3 bucket

GRANT USAGE ON INTEGRATION epc_s3_int TO ROLE TRANSFORM; -- grant usage to TRANSFORM role

-- Set defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE EPC_DB;
USE SCHEMA RAW;

-- create a file format
CREATE OR REPLACE FILE FORMAT epc_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL') -- 'N/A', 'NO DATA!', 'Not recorded', 'INVALID' are being left out for data profiling to perform anomaly detection if necessary
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TRIM_SPACE = TRUE
    ESCAPE_UNENCLOSED_FIELD = NONE
    DATE_FORMAT = 'AUTO'
    TIME_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    COMPRESSION = 'AUTO';

GRANT USAGE ON FILE FORMAT epc_csv_format TO ROLE TRANSFORM; -- grant usage to TRANSFORM role

-- create stages
CREATE OR REPLACE STAGE epc_raw_stage
  URL = 's3://epc-snowflake-project/raw/'
  STORAGE_INTEGRATION = epc_s3_int
  FILE_FORMAT = (FORMAT_NAME = epc_csv_format);

GRANT USAGE ON STAGE epc_raw_stage TO ROLE TRANSFORM;

-- create sequence for audit table

CREATE OR REPLACE SEQUENCE RAW_COPY_AUDIT_SEQ
    START = 1000
    INCREMENT = 1;

GRANT USAGE ON SEQUENCE RAW_COPY_AUDIT_SEQ TO ROLE TRANSFORM;

-- verify grants granted to the role before staring the ELT process
SHOW GRANTS TO ROLE TRANSFORM;