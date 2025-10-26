-- Snowflake SQL script to copy data from S3 stage to raw tables, this is an example, will be executed via Airflow

LIST @epc_raw_stage;

COPY INTO RAW_DISPLAY_RECOMMENDATIONS
FROM @epc_raw_stage/recommendations/display/display_recommendations_2025-08.csv
FILE_FORMAT = (FORMAT_NAME = EPC_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

COPY INTO RAW_DISPLAY_CERTIFICATES
FROM @epc_raw_stage/certificates/display/display_certificates_2025-08.csv
FILE_FORMAT = (FORMAT_NAME = EPC_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

COPY INTO RAW_DOMESTIC_CERTIFICATES
FROM @epc_raw_stage/certificates/domestic/domestic_certificates_2025-08.csv
FILE_FORMAT = (FORMAT_NAME = EPC_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

COPY INTO RAW_DOMESTIC_RECOMMENDATIONS
FROM @epc_raw_stage/recommendations/domestic/domestic_recommendations_2025-08.csv
FILE_FORMAT = (FORMAT_NAME = EPC_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

COPY INTO RAW_NON_DOMESTIC_CERTIFICATES
FROM @epc_raw_stage/certificates/non_domestic/non_domestic_certificates_2025-08.csv
FILE_FORMAT = (FORMAT_NAME = EPC_CSV_FORMAT)
ON_ERROR = 'CONTINUE';

COPY INTO RAW_NON_DOMESTIC_RECOMMENDATIONS
FROM @epc_raw_stage/recommendations/non_domestic/non_domestic_recommendations_2025-08.csv
FILE_FORMAT = (FORMAT_NAME = EPC_CSV_FORMAT)
ON_ERROR = 'CONTINUE';