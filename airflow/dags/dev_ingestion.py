from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from src.ingestion.epc_ingestion import fetch_files, process_files, s3_upload, cleanup

# Airflow executes your DAG scripts inside a container whose default 
# Python path does not include every arbitrary code directory. By default,
# Standard library modules, packages installed via pip or uv within the container, 
# or files in the same directory as the running script (/opt/airflow/dags in Airflow).

default_args = {
    "retries": 1,
    'retry_delay': timedelta(minutes=5)
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
    
    start = EmptyOperator(task_id='start')

    @task(task_id="fetch_files")
    def _fetch_files():
        """Fetch raw files (e.g., ZIPs) from API source."""
        return fetch_files()

    @task(task_id="process_files")
    def _process_files():
        """Unzip and process downloaded files."""
        return process_files()

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
        wait_for_completion=False,
        reset_dag_run=True,  # avoids duplicate triggers if DAG is rerun
        trigger_rule="all_success",
        conf={"latest_files": "{{ ti.xcom_pull(task_ids='upload_to_s3') }}"}
    )

    end = EmptyOperator(task_id='end')

    # Instantiate tasks
    fetch = _fetch_files()
    process = _process_files()
    upload = _upload_to_s3(process)
    clean = _cleanup_local()

    # Define task dependencies
    start >> fetch >> process >> upload >> clean >> trigger_next_dag >> end

# Instantiate the DAG
dev_ingestion_dag = dev_ingestion()