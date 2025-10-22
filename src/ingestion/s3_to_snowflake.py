from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.session import provide_session
from pathlib import Path
import os
from dotenv import load_dotenv
import sys
sys.path.append('/opt/airflow')

from src.utils.logger import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

logger = get_logger("src.utils.snowflake", log_dir=BASE_DIR / "logs", log_file="snowflake.log")

@provide_session
def create_snowflake_connection(conn_id: str="snowflake_epc_default", session=None):
    """
    Create or update a Snowflake Airflow connection using environment variables.
    Uses the Airflow Connection class directly (no subprocess, no CLI).

    Parameters
    ----------
    conn_id : str
        The connection ID to register in Airflow.
    session : Session
        Auto-injected by Airflow through @provide_session.
    """

    # Read from environment variables
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    database = os.getenv("SNOWFLAKE_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    role = os.getenv("SNOWFLAKE_ROLE")
    region = os.getenv("SNOWFLAKE_REGION")

    # Validate
    missing = [k for k, v in {
        "SNOWFLAKE_USER": user,
        "SNOWFLAKE_PASSWORD": password,
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_DB": database,
        "SNOWFLAKE_SCHEMA": schema,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_ROLE": role,
        "SNOWFLAKE_REGION": region
    }.items() if not v]

    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")
    
    conn_id = conn_id.replace(" ", "_")

    # Construct Connection URI using Airflow's Connection class
    conn = Connection(
        conn_id=conn_id,
        conn_type="snowflake",
        login=user,
        password=password,
        host=account,
        schema=schema,
        extra={
            "account": account,
            "warehouse": warehouse,
            "database": database,
            "role": role,
            "region": region
        }
    )

    # If connection exists, update; else add
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
    if existing_conn:
        logger.info(f"Found an existing connection: {existing_conn}, deleting it.")
        session.delete(existing_conn)
        session.commit()

    session.add(conn)
    session.commit()
    logger.info(f"Snowflake connection '{conn.conn_id}' created or updated successfully.")

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

def copy_and_update(unprocessed_files: list[str], dag_run_id: str):
    """
    Copy data files from S3 stage into appropriate Snowflake tables
    and log copy events into COPY_AUDIT table.
    """
    table_map = {
        "certificates/display": "DISPLAY_CERTIFICATES",
        "certificates/domestic": "DOMESTIC_CERTIFICATES",
        "certificates/non_domestic": "NON_DOMESTIC_CERTIFICATES",
        "recommendations/display": "DISPLAY_RECOMMENDATIONS",
        "recommendations/domestic": "DOMESTIC_RECOMMENDATIONS",
        "recommendations/non_domestic": "NON_DOMESTIC_RECOMMENDATIONS",
    }

    connection_id = os.getenv("SNOWFLAKE_CONNECTION", "snowflake_epc_default")
    logger.info(f"Using Snowflake connection ID: {connection_id}")
    hook = SnowflakeHook(snowflake_conn_id=connection_id)
    conn = None
    cur = None
    prefix = "s3://epc-snowflake-project/raw/"

    try:
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("USE DATABASE EPC_DB;")
        cur.execute("USE SCHEMA RAW;")
        cur.execute("USE WAREHOUSE COMPUTE_WH;")

        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();")
        db, schema, warehouse = cur.fetchone()
        logger.info(f"Connected to Snowflake DB: {db}, Schema: {schema}, Warehouse: {warehouse}")

        for file_uri in unprocessed_files:
            try:
                if not file_uri.startswith(prefix):
                    logger.warning(f"File URI does not match expected S3 prefix, skipping: {file_uri}")
                    continue

                matched_key = next((k for k in table_map if k in file_uri), None)
                if not matched_key:
                    logger.warning(f"No mapping found for file: {file_uri}")
                    continue

                target_table = table_map[matched_key]
                relative = file_uri[len(prefix):] # uri - prefix = "certificates/display/<filename>"
                subdir, filename = relative.rsplit("/", 1) # split into "certificates/display" and "<filename>"

                # COPY command
                copy_sql = f"""
                    COPY INTO {target_table}
                    FROM @epc_raw_stage/{subdir}/{filename}
                    FILE_FORMAT = (FORMAT_NAME = epc_csv_format)
                    ON_ERROR = 'CONTINUE';
                """

                logger.info(f"Running COPY for {file_uri} → {target_table}")
                cur.execute(copy_sql)
                logger.info(f"Completed copying the file {file_uri}.")
                result = cur.fetchone()

                (
                    meta_file, status, rows_parsed, rows_loaded, error_limit, errors_seen,
                    first_error, first_error_line, first_error_character, first_error_column_name
                ) = result if result else (file_uri, "UNKNOWN", 0, 0, None, None, None, None, None, None)

                # AUDIT insert (extended metadata)
                audit_sql = f"""
                INSERT INTO COPY_AUDIT (
                    FILEPATH,
                    TARGET_TABLE,
                    STATUS,
                    ROWS_PARSED,
                    ROWS_LOADED,
                    ERROR_LIMIT,
                    ERRORS_SEEN,
                    FIRST_ERROR,
                    FIRST_ERROR_LINE,
                    FIRST_ERROR_CHARACTER,
                    FIRST_ERROR_COLUMN_NAME,
                    LOAD_TS,
                    DAG_RUN_ID
                ) VALUES (
                    {repr(file_uri)},
                    {repr(target_table)},
                    {repr(status)},
                    {rows_parsed},
                    {rows_loaded},
                    {error_limit if error_limit is not None else 'NULL'},
                    {errors_seen if errors_seen is not None else 'NULL'},
                    {repr(first_error) if first_error is not None else 'NULL'},
                    {first_error_line if first_error_line is not None else 'NULL'},
                    {first_error_character if first_error_character is not None else 'NULL'},
                    {repr(first_error_column_name) if first_error_column_name is not None else 'NULL'},
                    CURRENT_TIMESTAMP,
                    {repr(dag_run_id)}
                );
                """
                cur.execute(audit_sql)
                logger.info(f"Completed updating COPY_AUDIT with metadata: {result}.")
                conn.commit()

            except Exception as e:
                logger.error(f"Error copying {file_uri}: {e}", exc_info=True)
                conn.rollback()
                raise AirflowException(f"Failed to copy {file_uri}: {e}")
    
    except Exception as e:
        logger.error(f"Error while checking COPY_AUDIT table: {e}", exc_info=True)
        raise AirflowException(f"Failed to query COPY_AUDIT: {str(e)}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__=="__main__":
    create_snowflake_connection()