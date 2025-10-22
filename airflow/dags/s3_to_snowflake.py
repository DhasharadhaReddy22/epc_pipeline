import ast
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta, timezone
import sys
sys.path.append('/opt/airflow')

from src.ingestion.epc_ingestion import list_latest_files
from src.ingestion.s3_to_snowflake import check_audit_table, copy_and_update

default_args = {
    "retries": 1,
    'retry_delay': timedelta(minutes=1)
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
def dev_s3_to_snowflake():

    @task(task_id="start")
    def _start():
        print("Starting DAG execution")
    
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

    @task.branch(task_id="branch_on_files")
    def _branch_on_files(unprocessed: list[str]):
        """Branch based on whether unprocessed files exist."""
        print(f"Branching on unprocessed files: {unprocessed}")
        if not unprocessed:
            return "skip_copy"
        return "trigger_copy_files"

    @task(task_id="skip_copy")
    def _skip_copy():
        print("Skipping copy as there are no new files to process.")
        return "skip_copy"

    @task(task_id="trigger_copy_files")
    def _trigger_copy_files(unprocessed: list[str]):
        """Just acts as a static bridge for branching"""
        print("Triggering dynamic copies for:", unprocessed)
        return unprocessed  # pass list downstream

    @task(task_id="copy_file")
    def _copy_file(file_path: str, dag_run=None):
        """Copy a file to Snowflake and update COPY_AUDIT."""
        dag_run_id = dag_run.run_id if dag_run else f"manual__{datetime.now(timezone.utc).isoformat()}"
        print(f"Copying {file_path} for DAG run {dag_run_id}")
        copy_and_update([file_path], dag_run_id)

    @task(task_id="end", trigger_rule="none_failed_min_one_success")
    def _end():
        print("DAG completed successfully")

    # Instantiating task instances, no need to do this for operator based task definitions
    start = _start()
    latest_files = _get_latest_files()
    check_copy_audit = _check_copy_audit(latest_files)
    branch_task = _branch_on_files(check_copy_audit)
    skip_copy = _skip_copy()

    trigger_copy_files = _trigger_copy_files(check_copy_audit)
    copy_file = _copy_file.expand(file_path=trigger_copy_files)
    # not _copy_file().expand(...) since we do not want to call _copy_file yet and have the implicit MappedOperator generate the N tasks
    
    end = _end()

    # DAG sequence, decorator tasks are already linked via above instantiations
    start >> latest_files >> check_copy_audit >> branch_task
    # branching logic is shown here
    branch_task >> skip_copy >> end
    branch_task >> trigger_copy_files >> copy_file >> end

# Instantiate the DAG
dev_s3_to_snowflake_dag = dev_s3_to_snowflake()