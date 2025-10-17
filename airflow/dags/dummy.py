from airflow import DAG
from airflow.operators.empty import EmptyOperator
from docker.types import Mount
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/python_scripts')
# Airflow executes your DAG scripts inside a container whose default 
# Python path does not include every arbitrary code directory. By default,
# Standard library modules, packages installed via pip within the container, 
# or files in the same directory as the running script (/opt/airflow/dags in Airflow).

default_args = {
    'Description': 'DAG to orchestrate data',
    'start_date': datetime(2025, 7, 30),
    'catchup': False
}

with DAG(
    dag_id = 'dummy',
    default_args = default_args,
    schedule = timedelta(minutes = 1),
    description = "A simple DAG to pick up data from the api and load it to the DB",
    tags = ["example"]
) as dag:

    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    # Set dependencies: start -> branches -> merge -> end
    start >> end