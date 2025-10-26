from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="dev_dbt_transformations",
    description="A dummy downstream DAG triggered by upstream DAG",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Only triggered manually or by another DAG
    catchup=False,
    tags=["downstream", "triggered"]
)
def dev_dbt_transformations():

    @task(task_id="start")
    def start_task():
        print("Starting downstream DAG execution...")
    
    @task(task_id="print_status")
    def print_status():
        print("Running data transformations (dummy)...")
    
    @task(task_id="end")
    def end_task():
        print("Downstream DAG completed successfully!")

    # Task dependencies
    start_task() >> print_status() >> end_task()

# Create DAG instance
downstream_dag = dev_dbt_transformations()