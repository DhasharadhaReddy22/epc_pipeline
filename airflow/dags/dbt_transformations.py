from datetime import datetime, timedelta, timezone
import sys
import re

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowFailException
from docker.types import Mount

sys.path.append('/opt/airflow')

default_args = {
    "retries": 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id="dev_dbt_transformations",
    default_args=default_args,
    description="A downstream DBT transformations and test DAG triggered by upstream s3_to_snowflake DAG",
    start_date=datetime(2025, 7, 30),
    schedule=None,
    catchup=False,
    tags=["dbt", "transformations", "tests"]
)
def dev_dbt_transformations():

    @task(task_id="start")
    def _start():
        print("Starting downstream DAG execution...")

    @task(task_id="raw_tests")
    def _raw_tests(**context):
        """
        Runs dbt test (tag:raw) inside a Docker container.
        Cleans ANSI escape codes, fails if dbt fails, and returns summary line.
        """

        docker_operator = DockerOperator(
            task_id="dbt_epc_raw_tests",
            image="dbt-snowflake:custom",
            api_version="auto",
            auto_remove="success",
            command=(
                # tee /tmp/dbt_output.log → Captures the output of the dbt test command into a log file while also displaying it in real-time.
                # status=$? → Stores the exit code of the dbt test command (0 for success, non-zero for failure).
                # tail -n 1 /tmp/dbt_output.log → Outputs the last line of the log file, which contains the summary of the dbt test results.
                # exit $status → Exits the shell with the original dbt exit code.
                "bash -c 'dbt test --select tag:raw | tee /tmp/dbt_output.log; "
                "status=$?; tail -n 1 /tmp/dbt_output.log; exit $status'"
            ),
            docker_url="unix://var/run/docker.sock",
            network_mode="epc_pipeline_epc-network",
            working_dir="/usr/app",
            mount_tmp_dir=False,
            do_xcom_push=True,
            mounts=[
                Mount(
                    source="/home/dhasharadhareddyb/projects/epc_pipeline/dbt/dbt_epc",
                    target="/usr/app",
                    type="bind",
                ),
                Mount(
                    source="/home/dhasharadhareddyb/projects/epc_pipeline/dbt/profiles.yml",
                    target="/home/dbt_user/.dbt/profiles.yml",
                    type="bind",
                ),
            ],
            environment={"DBT_PROFILES_DIR": "/home/dbt_user/.dbt"},
        )

        # Execute the DockerOperator within the task context
        try:
            result = docker_operator.execute(context=context)
            # Cleaning the output to only the stats
            cleaned_output = str.split(result, "Done.")[1].strip()
            print(f"DBT RAW TEST SUMMARY: {cleaned_output}")

            # If dbt tests show errors, mark the task as failed
            if "ERROR=0" not in cleaned_output:
                try:
                    error_count = int(re.search(r"ERROR=(\d+)", cleaned_output).group(1))
                    if error_count > 0:
                        raise AirflowFailException(f"DBT tests failed with {error_count} errors: {cleaned_output}")
                except Exception:
                    # If regex parsing fails, still raise
                    raise AirflowFailException(f"DBT tests failed: {cleaned_output}")
             
        except Exception as e:
            raise AirflowFailException(f"DBT test Docker task failed: {e}")

        return cleaned_output
    
    @task(task_id="print_status")
    def _print_status(summary: str):
        print(f"Previous test result summary: {summary}")
    
    @task(task_id="end")
    def _end_task():
        print("Downstream DAG completed successfully!")

    start = _start()
    raw_tests = _raw_tests()
    print_status = _print_status(raw_tests)
    end = _end_task()

    # DAG dependencies
    start >> raw_tests >> print_status >> end

# Create DAG instance
downstream_dag = dev_dbt_transformations()