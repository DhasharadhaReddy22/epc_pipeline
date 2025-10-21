from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()

from src.utils.logger import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent

logger = get_logger("src.utils.snowflake", log_dir=BASE_DIR / "logs", log_file="snowflake.log")

def check_audit_table(s3_paths: list[str]) -> list[str]:
    """
    Returns subset of s3_paths that have not yet been processed
    (missing in RAW.COPY_AUDIT table).
    """
    if not s3_paths:
        logger.info("No file paths provided to check in COPY_AUDIT.")
        return []

    connection_id = os.getenv("SNOWFLAKE_CONNECTION", "snowflake_epc_default")
    logger.info(f"Using Snowflake connection ID: {connection_id}")
    hook = SnowflakeHook(snowflake_conn_id=connection_id)
    conn = None
    cur = None

    try:
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("USE DATABASE EPC_DB;")
        cur.execute("USE SCHEMA RAW;")
        cur.execute("USE WAREHOUSE COMPUTE_WH;")

        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();")
        db, schema, warehouse = cur.fetchone()
        logger.info(f"Connected to Snowflake DB: {db}, Schema: {schema}, Warehouse: {warehouse}")
        
        formatted = ",".join([f"'{filepath}'" for filepath in s3_paths])
        query = f"""
            SELECT FILEPATH 
            FROM COPY_AUDIT
            WHERE FILEPATH IN ({formatted});
        """

        logger.info(f"Executing COPY_AUDIT check query:\n{query}")
        cur.execute(query)

        processed = {row[0] for row in cur.fetchall()}
        unprocessed = [filepath for filepath in s3_paths if filepath not in processed]
        return unprocessed

    except Exception as e:
        logger.error(f"Error while checking COPY_AUDIT table: {e}", exc_info=True)
        raise AirflowException(f"Failed to query COPY_AUDIT: {str(e)}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def generate_copy_sql(file_path: str, **context) -> str:
    """
    Generate COPY INTO SQL for a given S3 file path
    and corresponding INSERT into COPY_AUDIT.
    """
    dag_run_id = context["dag_run"].run_id
    prefix = "s3://epc-snowflake-project/raw/"
    if not file_path.startswith(prefix):
        raise ValueError("Unexpected s3 path format")

    relative = file_path[len(prefix):]  # e.g. "recommendations/domestic/display_recommendations_2025-08.csv"
    parts = relative.split("/")
    subdir = "/".join(parts[:-1])  # e.g. "recommendations/domestic"
    filename = parts[-1]

    table_map = {
        "certificates/display": "DISPLAY_CERTIFICATES",
        "certificates/domestic": "DOMESTIC_CERTIFICATES",
        "certificates/non_domestic": "NON_DOMESTIC_CERTIFICATES",
        "recommendations/display": "DISPLAY_RECOMMENDATIONS",
        "recommendations/domestic": "DOMESTIC_RECOMMENDATIONS",
        "recommendations/non_domestic": "NON_DOMESTIC_RECOMMENDATIONS",
    }

    target_table = table_map.get(subdir)
    if not target_table:
        raise ValueError(f"No table mapping for subdir: {subdir}")
    
    sql = f"""
        COPY INTO {target_table}
        FROM @epc_raw_stage/{subdir}/{filename}
        FILE_FORMAT = (FORMAT_NAME = epc_csv_format)
        ON_ERROR = 'CONTINUE';

        INSERT INTO COPY_AUDIT (FILEPATH, TARGET_TABLE, LOAD_TS, DAG_RUN_ID)
        VALUES ('{file_path}', '{target_table}', CURRENT_TIMESTAMP, '{dag_run_id}');
    """
    return sql