from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from src.ingestion.epc_ingestion import fetch_files, process_files, s3_upload, cleanup

# Airflow executes your DAG scripts inside a container whose default 
# Python path does not include every arbitrary code directory. By default,
# Standard library modules, packages installed via pip or uv within the container, 
# or files in the same directory as the running script (/opt/airflow/dags in Airflow).

default_args = {
    'Description': 'Dev Ingestion DAG with Custom S3 Scripts',
    'catchup': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = 'dev_ingestion',
    default_args = default_args,
    schedule = timedelta(minutes=30),
    start_date = datetime(2025, 7, 30),
    description = "Monthly DAG to download and process zip files and upload to s3 using custom python scripts"
) as dag:
    
    start = EmptyOperator(task_id='start')

    fetch_task = PythonOperator(
        task_id="fetch_files",
        python_callable=fetch_files
    )

    process_task = PythonOperator(
        task_id="process_files",
        python_callable=process_files # returns processed_files dict
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=lambda ti: s3_upload(ti.xcom_pull(task_ids="process_files")) 
        # lambda function is to avoid having to write a separate function just to pass the xcom
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_local",
        python_callable=cleanup
    )

    end = EmptyOperator(task_id='end')

    start >> fetch_task >> process_task >> upload_task >> cleanup_task >> end