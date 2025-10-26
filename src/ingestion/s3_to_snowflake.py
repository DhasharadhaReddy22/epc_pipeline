import os
import sys
from pathlib import Path
from typing import Dict
from datetime import datetime, timezone
from dotenv import load_dotenv

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.session import provide_session

sys.path.append('/opt/airflow')

from src.utils.logger import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

logger = get_logger("src.ingestion.s3_to_snowflake", log_dir=BASE_DIR / "logs", log_file="snowflake.log")

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
    (missing in RAW.RAW_COPY_AUDIT table).

    This is done despite the default behaviour of Snowflake COPY
    to skip duplicates, to avoid unnecessary processing and logging
    even if the copy was forced, using the RAW_COPY_AUDIT table 
    we can get rid of the duplicates if required, so a useful step to have.
    """
    if not s3_paths:
        logger.info("No file paths provided to check in RAW.RAW_COPY_AUDIT.")
        return []

    connection_id = os.getenv("SNOWFLAKE_CONNECTION", "snowflake_epc_default")
    hook = SnowflakeHook(snowflake_conn_id=connection_id)
    conn = None
    cur = None

    try:
        conn = hook.get_conn()
        cur = conn.cursor()
        logger.info(f"Using Snowflake connection ID: {connection_id}")

        cur.execute("USE WAREHOUSE COMPUTE_WH;")
        cur.execute("USE DATABASE EPC_DB;")
        cur.execute("USE SCHEMA RAW;")

        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();")
        db, schema, warehouse = cur.fetchone()
        logger.info(f"Connected to Snowflake DB: {db}, Schema: {schema}, Warehouse: {warehouse}")
        
        formatted = ",".join([f"'{filepath}'" for filepath in s3_paths])
        query = f"""
            SELECT FILEPATH 
            FROM RAW_COPY_AUDIT
            WHERE FILEPATH IN ({formatted});
        """

        logger.info(f"Executing RAW_COPY_AUDIT check query:\n{query}")
        cur.execute(query)

        processed = {row[0] for row in cur.fetchall()}
        unprocessed = [filepath for filepath in s3_paths if filepath not in processed]
        return unprocessed

    except Exception as e:
        logger.error(f"Error while checking RAW_COPY_AUDIT table: {e}", exc_info=True)
        raise AirflowException(f"Failed to query RAW_COPY_AUDIT: {str(e)}")

    finally:
        if cur: cur.close()
        if conn: conn.close()

def _perform_copy(cur, target_table, subdir, filename):
    """
    Execute a standard COPY INTO command for a given target table and file.
    Returns the result of the COPY operation.
    """
    copy_sql = f"""
        COPY INTO {target_table}
        FROM @epc_raw_stage/{subdir}/{filename}
        FILE_FORMAT = (FORMAT_NAME = epc_csv_format)
        ON_ERROR = 'CONTINUE';
    """
    logger.info(f"Running COPY for {filename} → {target_table}")
    cur.execute(copy_sql)
    result = cur.fetchone()
    logger.info(f"Completed copying the file {filename}. Result: {result}")
    return result or (f"NO RESULT — possible COPY failure for {filename}",)

def _force_copy(cur, target_table, subdir, filename):
    """
    Executes a COPY INTO command with FORCE=TRUE, ensuring reload of files 
    already marked as loaded in COPY_HISTORY. Used after partial or failed copies.
    """
    copy_sql = f"""
        COPY INTO {target_table}
        FROM @epc_raw_stage/{subdir}/{filename}
        FILE_FORMAT = (FORMAT_NAME = epc_csv_format)
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
    """
    logger.info(f"Running FORCED COPY for {filename} → {target_table}")
    cur.execute(copy_sql)
    result = cur.fetchone()
    logger.info(f"Completed copying the file {filename}. Result: {result}")
    return result or (f"NO RESULT — possible COPY failure for {filename}",)

def _insert_audit_record(cur, 
                 audit_id: str,
                 file_uri: str,
                 target_table: str,
                 dag_run_id: str,
                 result: tuple):
    """
    Inserts an entry into RAW_COPY_AUDIT table capturing COPY operation metadata.
    It inserts other metadata like audit_id, file_uri, target_table, and the dag_run_id.
    """

    (
        meta_file, status, rows_parsed, rows_loaded, error_limit, errors_seen,
        first_error, first_error_line, first_error_character, first_error_column_name
    ) = result if result else (file_uri, "UNKNOWN", 0, 0, None, None, None, None, None, None)

    audit_sql = """
        INSERT INTO RAW_COPY_AUDIT (
            AUDIT_ID,
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
            AUDIT_TS,
            DAG_RUN_ID
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,%s)
    """
    audit_values = (audit_id, file_uri, target_table, status, rows_parsed, rows_loaded, error_limit,
                   errors_seen, first_error, first_error_line, first_error_character, 
                   first_error_column_name, dag_run_id)
    
    cur.execute(audit_sql, audit_values)
    logger.info(f"Inserted audit for {file_uri} with status {status}.")

def _check_if_any_null_audit_id(cur, target_table: str) -> bool:
    """
    Checks if there are any rows in the target table with NULL AUDIT_ID.
    Returns True if such rows exist, else False.
    """
    check_sql = f"""
        SELECT COUNT(*) 
        FROM {target_table} 
        WHERE AUDIT_ID IS NULL;
    """
    cur.execute(check_sql)
    count = cur.fetchone()[0]
    has_nulls = count > 0
    if has_nulls:
        logger.warning(f"Found {count} rows in {target_table} with NULL AUDIT_ID.")
    else:
        logger.info(f"No rows with NULL AUDIT_ID found in {target_table}.")
    return has_nulls

def _update_target_table_audit_id(cur, target_table: str, audit_id:str):
    """
    Updates newly inserted rows in the target table by attaching the given AUDIT_ID.
    """
    update_sql = f"""
        UPDATE {target_table}
        SET AUDIT_ID = {audit_id}
        WHERE AUDIT_ID IS NULL;
    """
    cur.execute(update_sql)
    logger.info(f"Updated {target_table} records with AUDIT_ID={audit_id}.")

def _delete_unaudited_rows(cur, target_table):
    """
    Deletes rows that were inserted by COPY INTO but never associated with an AUDIT_ID.
    Used for manual cleanup when an audit insert fails mid-transaction and rollback works only on 
    audit and update sqls but not copy as it is auto-commited internally and recorded in copy_history.
    """
    delete_sql = f"""
    DELETE FROM {target_table} 
    WHERE AUDIT_ID IS NULL;
    """
    cur.execute(delete_sql)
    logger.warning(f"Deleted unaudited rows from {target_table}.")

def copy_and_update(unprocessed_files: list[str], dag_run_id: str) -> Dict[str, str]:
    """
    Performs COPY INTO from S3 stage → Snowflake RAW tables, logs each copy
    to RAW_COPY_AUDIT, and enforces cleanup on partial failure.
    """
    
    table_map = {
        "certificates/display": "RAW_DISPLAY_CERTIFICATES",
        "certificates/domestic": "RAW_DOMESTIC_CERTIFICATES",
        "certificates/non_domestic": "RAW_NON_DOMESTIC_CERTIFICATES",
        "recommendations/display": "RAW_DISPLAY_RECOMMENDATIONS",
        "recommendations/domestic": "RAW_DOMESTIC_RECOMMENDATIONS",
        "recommendations/non_domestic": "RAW_NON_DOMESTIC_RECOMMENDATIONS",
    }

    connection_id = os.getenv("SNOWFLAKE_CONNECTION", "snowflake_epc_default")
    logger.info(f"Using Snowflake connection ID: {connection_id}")
    hook = SnowflakeHook(snowflake_conn_id=connection_id)
    conn, cur = None, None

    prefix = "s3://epc-snowflake-project/raw/"
    copy_results = {} # return status per file

    try:
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE COMPUTE_WH;")
        cur.execute("USE DATABASE EPC_DB;")
        cur.execute("USE SCHEMA RAW;")
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();")
        db, schema, warehouse = cur.fetchone()
        logger.info(f"Connected to Snowflake DB: {db}, Schema: {schema}, Warehouse: {warehouse}")

        for file_uri in unprocessed_files:
            retry_once = False  # so that we do not keep retrying forever

            try:
                if not file_uri.startswith(prefix):
                    logger.warning(f"File URI does not match expected S3 prefix, skipping: {file_uri}")
                    continue

                matched_key = next((key for key in table_map if key in file_uri), None)
                if not matched_key:
                    logger.warning(f"No table mapping found for file: {file_uri}")
                    continue

                target_table = table_map[matched_key]
                relative = file_uri[len(prefix):] # uri - prefix = "certificates/display/<filename>"
                subdir, filename = relative.rsplit("/", 1) # split into "certificates/display" and "<filename>"
                
                # Generate new AUDIT_ID
                cur.execute("SELECT RAW_AUDIT_ID_SEQ.NEXTVAL;")
                audit_id = cur.fetchone()[0]

                # First attempt at copying
                result = _perform_copy(cur, target_table, subdir, filename)

                while (len(result) == 1):
                    if retry_once:
                        logger.error(f"Second COPY attempt also incomplete for {file_uri}. Aborting further retries and deleting rows from recent copy.")
                        _delete_unaudited_rows(cur, target_table)
                        
                        # Move on to the next file; skip audit for this one
                        raise AirflowException(f"Force copy also failed for {file_uri}.")
                    
                    logger.warning(f"COPY result: {result[0]}. This happens when the copy was successful but audit failed or the copy has failed.")
                    logger.info("Performing deletion of rows that have been inserted without audit_id due to failed audit.")

                    if not _check_if_any_null_audit_id(cur, target_table): # this is done so that we will not end up force copying unnecessarily
                        logger.warning(f"No unaudited rows, yet returned {result[0]}. Must be an issue with the RAW_COPY_AUDIT table.")
                        # Move on to the next file; skip audit for this one
                        raise AirflowException(f"Issue with RAW_COPY_AUDIT table for {file_uri}.")
                    else:
                        _delete_unaudited_rows(cur, target_table)
                        result = _force_copy(cur, target_table, subdir, filename)
                    retry_once = True

                _insert_audit_record(cur, audit_id, file_uri, target_table, dag_run_id, result)
                _update_target_table_audit_id(cur, target_table, audit_id)
                
                conn.commit() # single commit per file after copy and audit insert, so it becomes atomic
                logger.info(f"File {file_uri} copy completed with status: {result[1]}")
                
                copy_results[file_uri] = result[1]

            except Exception as e:
                logger.error(f"Error while copying or auditing {file_uri}: {e}", exc_info=True)
                if 'target_table' in locals(): # Check if target_table is defined else you will hit a UnboundLocalError
                    _delete_unaudited_rows(cur, target_table)
                conn.rollback()

                copy_results[file_uri] = "FAILED"
                continue # to next file_uri in list
            
        return copy_results
    
    except Exception as e:
        logger.error(f"Snowflake operation failed: {e}", exc_info=True)
        raise AirflowException(f"Failed to perform copy operations: {str(e)}")

    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__=="__main__":
    create_snowflake_connection()