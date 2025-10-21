from airflow.decorators import dag, task
from datetime import datetime

@dag(
    description="A dynamic DAG to process files", 
    start_date=datetime(2025, 1, 1), 
    schedule=None, 
    catchup=False
    )

def dynamic_dag():

    @task()
    def get_file_list():
        return ["file1.csv", "file2.csv", "file3.csv"]

    @task()
    def process(file):
        print(f"Processing {file}")

    process.expand(file=get_file_list())  # <-- Dynamic Task Mapping

dynamic_dag()