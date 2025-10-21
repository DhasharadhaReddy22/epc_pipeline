import ast
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from src.ingestion.epc_ingestion import list_latest_files
from src.ingestion.s3_to_snowflake import check_audit_table

default_args = {
    "retries": 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="dev_s3_to_snowflake",
    default_args = default_args,
    start_date = datetime(2025, 7, 30),
    schedule = None,
    description = "DAG to check COPY_AUDIT and copy latest CSVs from S3 to Snowflake that have not been copied yet",
    catchup=False,
    tags=["s3", "snowflake"]
)
def s3_to_snowflake():

    start = EmptyOperator(task_id="start")
    
    @task(task_id="get_latest_files")
    def _get_latest_files(dag_run=None):
        """Get the latest files uploaded for this month from S3."""
        if dag_run and dag_run.conf:
            conf_files = dag_run.conf.get("latest_files")
            print("Checking for files: ", type(conf_files), conf_files)
            if isinstance(conf_files, str):
                try:
                    conf_files = ast.literal_eval(conf_files)
                except Exception as e:
                    print("Failed to parse conf_files, using empty list:", e)
                    conf_files = []
            if len(conf_files)==0:
                print("No files found in conf, fetching from S3 to double check")
                return list_latest_files()
            print(f"Using conf files from parent DAG: {conf_files}")
            return conf_files
        else:
            print("No conf provided, fetching from S3")
            return list_latest_files()

    @task(task_id="check_copy_audit")
    def _check_copy_audit(latest_files: list[str]):
        """Check which of the latest files have not been copied to Snowflake yet."""
        print(f"Latest files to check: {latest_files}")
        if len(latest_files) == 0:
            print("No latest files to check.")
            return []

        try:
            unprocessed_files = check_audit_table(latest_files)
            return unprocessed_files

        except Exception as e:
            print(f"Error in check_if_copied: {e}")
            raise AirflowException(str(e))

    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    latest_files = _get_latest_files()
    check_copy_audit = _check_copy_audit(latest_files)
    
    # DAG sequence
    start >> latest_files >> check_copy_audit >> end

# Instantiate the DAG
s3_to_snowflake_dag = s3_to_snowflake()