from datetime import datetime, timedelta, timezone
import sys
import re

from airflow.decorators import dag, task, task_group
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowFailException
from docker.types import Mount

sys.path.append('/opt/airflow')

def _run_dbt_in_docker(task_id, command, context):
    """
    Runs a dbt command inside a Docker container.
    The command passed is a composite bash command that captures output, exit status, and returns summary.
    Example: "bash -c 'dbt test --select tag:raw | tee /tmp/dbt_output.log; status=$?; tail -n 1 /tmp/dbt_output.log; exit $status'"
        tee /tmp/dbt_output.log → Captures the output of the dbt test command into a log file while also displaying it in real-time.
        status=$? → Stores the exit code of the dbt test command (0 for success, non-zero for failure).
        tail -n 1 /tmp/dbt_output.log → Outputs the last line of the log file, which contains the summary of the dbt test results.
        exit $status → Exits the shell with the original dbt exit code.
    """

    docker_operator = DockerOperator(
        task_id=task_id,
        image="dbt-snowflake:custom",
        api_version="auto",
        auto_remove="success",
        command=command,
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
    try:
        result = docker_operator.execute(context=context)
        # Cleaning the output to only the stats
        cleaned_output = str.split(result, "Done.")[-1].strip()
        print(f"DBT Execution Summary: {cleaned_output}")

        if "ERROR=0" not in cleaned_output:
            try:
                error_count = int(re.search(r"ERROR=(\d+)", cleaned_output).group(1))
                if error_count > 0:
                    raise AirflowFailException(f"DBT task failed with {error_count} errors: {cleaned_output}")
            except Exception:
                # If regex parsing fails, still raise
                raise AirflowFailException(f"DBT task failed: {cleaned_output}")

    except Exception as e:
        raise AirflowFailException(f"DBT task Docker task failed: {e}")

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
        try:
            return _run_dbt_in_docker(
                task_id="dbt_epc_raw_tests",
                command=(
                    "bash -c 'dbt test --select tag:raw | tee /tmp/dbt_output.log; "
                    "status=$?; tail -n 1 /tmp/dbt_output.log; exit $status'"
                ),
                context=context,
            )
        
        except Exception as e:
            raise AirflowFailException(f"DBT raw test task failed: {e}")
    
    @task_group(group_id="staging_tasks")
    def _staging_tasks():

        @task(task_id="stg_run")
        def _stg_run(**context):
            return _run_dbt_in_docker(
                task_id="dbt_epc_stg_run",
                command=(
                    "bash -c 'dbt run --select tag:staging | tee /tmp/dbt_output.log; "
                    "status=$?; tail -n 1 /tmp/dbt_output.log; exit $status'"
                ),
                context=context,
            )

        @task(task_id="stg_tests")
        def _stg_tests(**context):
            return _run_dbt_in_docker(
                task_id="dbt_epc_stg_tests",
                command=(
                    "bash -c 'dbt test --select tag:staging | tee /tmp/dbt_output.log; "
                    "status=$?; tail -n 1 /tmp/dbt_output.log; exit $status'"
                ),
                context=context,
            )

        stg_run = _stg_run()
        stg_tests = _stg_tests()
        stg_run >> stg_tests
    
    @task_group(group_id="presentation_tasks")
    def _presentation_tasks():

        @task(task_id="presentation_run")
        def _presentation_run(**context):
            return _run_dbt_in_docker(
                task_id="dbt_epc_presentation_run",
                command=(
                    "bash -c 'dbt run --select tag:presentation | tee /tmp/dbt_output.log; "
                    "status=$?; tail -n 1 /tmp/dbt_output.log; exit $status'"
                ),
                context=context,
            )

        @task(task_id="presentation_tests")
        def _presentation_tests(**context):
            return _run_dbt_in_docker(
                task_id="dbt_epc_presentation_tests",
                command=(
                    "bash -c 'dbt test --select tag:presentation | tee /tmp/dbt_output.log; "
                    "status=$?; tail -n 1 /tmp/dbt_output.log; exit $status'"
                ),
                context=context,
            )

        presentation_run = _presentation_run()
        presentation_tests = _presentation_tests()
        presentation_run >> presentation_tests
    
    @task(task_id="end")
    def _end_task():
        print("Downstream DAG completed successfully!")

    start = _start()
    raw_tests = _raw_tests()
    staging_tasks = _staging_tasks()
    presentation_tasks = _presentation_tasks()
    end = _end_task()

    # DAG dependencies
    start >> raw_tests >> staging_tasks >> presentation_tasks >> end

# Create DAG instance
downstream_dag = dev_dbt_transformations()