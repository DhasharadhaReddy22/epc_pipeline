from airflow.sdk import Variable
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from src.ingestion.epc_ingestion import fetch_files, process_files, s3_upload, cleanup, get_prev_month_string

# Airflow executes your DAG scripts inside a container whose default 
# Python path does not include every arbitrary code directory. By default,
# Standard library modules, packages installed via pip or uv within the container, 
# or files in the same directory as the running script (/opt/airflow/dags in Airflow).

def get_month_tag() -> str:
    """Retrieve the month tag from Airflow Variable, or default to previous month."""
    var_value = Variable.get("epc_month_tag (YYYY-MM)", default=None)
    if var_value is None:
        print(f"No month tag variable set, defaulting to latest month: {get_prev_month_string()}")
        return get_prev_month_string()
    try:
        parsed = datetime.strptime(var_value, "%Y-%m")
        month_tag = parsed.strftime("%Y-%m")
        return month_tag
    except ValueError:
        raise ValueError(f"Invalid month tag format in Airflow Variable: {var_value}. Expected 'YYYY-MM'.")

default_args = {
    "retries": 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id = 'dev_ingestion',
    default_args = default_args,
    start_date = datetime(2025, 7, 30),
    schedule = timedelta(minutes=30),
    description = "Monthly DAG to download and process zip files and upload to s3 using custom python scripts",
    catchup=False,
    tags=["epc", "ingestion"]
)
def dev_ingestion():
    
    @task(task_id="start")
    def _start():
        print("Starting DAG execution")

    @task(task_id="fetch_files")
    def _fetch_files():
        """Fetch raw files (e.g., ZIPs) from API source."""
        month_tag = get_month_tag()
        print(f"Fetching files for month: {month_tag}")
        return fetch_files(month_tag=month_tag)

    @task(task_id="process_files")
    def _process_files():
        """Unzip and process downloaded files."""
        month_tag = get_month_tag()
        print(f"Processing files for month: {month_tag}")
        return process_files(month_tag=month_tag)

    @task(task_id="upload_to_s3")
    def _upload_to_s3(processed_files: dict):
        """Upload processed files to S3."""
        return s3_upload(processed_files)

    @task(task_id="cleanup_local")
    def _cleanup_local():
        """Cleanup the temporary local files and directories."""
        return cleanup()

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_s3_to_snowflake",
        trigger_dag_id="dev_s3_to_snowflake",
        trigger_rule='all_success',
        wait_for_completion=False,
        reset_dag_run=True,
        # Jinja-templated pull of the list from upload_to_s3’s return value
        conf={"latest_files": "{{ ti.xcom_pull(task_ids='upload_to_s3') }}"}
    )

    @task(task_id="end")
    def _end():
        print("DAG completed successfully")

    # Instantiate tasks
    start = _start()
    fetch = _fetch_files()
    process = _process_files()
    upload = _upload_to_s3(process)
    clean = _cleanup_local()
    end = _end()

    # Define task dependencies
    start >> fetch >> process >> upload >> trigger_next_dag >> clean >> end

# Instantiate the DAG
dev_ingestion_dag = dev_ingestion()