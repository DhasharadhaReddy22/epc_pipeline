import ast
from airflow.sdk import Variable
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import sys
sys.path.append('/opt/airflow')

from src.ingestion.epc_ingestion import get_prev_month_string, list_latest_files
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
        """Get the latest or specific files uploaded for this month from S3."""
        conf_files = None
        month_tag = None

        # Check dag_run.conf first
        if dag_run and dag_run.conf:
            conf_files = dag_run.conf.get("latest_files")
            if (conf_files is None) or (len(conf_files) == 0):
                print("DAG conf latest_files is empty list.")
            # Parse if string, this is often the case with Airflow Variables and dag_run.conf
            elif isinstance(conf_files, str):
                print("Found DAG conf:", conf_files)
                try:
                    conf_files = ast.literal_eval(conf_files)
                    print("Using latest_files from DAG conf:", conf_files)
                    return conf_files
                
                except Exception as e:
                    print("Failed to parse conf_files, using empty list:", e)
                    conf_files = []

        # If not found or empty, check Airflow Variables, this is mostly for ad-hoc run
        if not conf_files:
            manual_list = Variable.get("manual_file_list", default=None)
            if manual_list:
                try:
                    conf_files = ast.literal_eval(manual_list)
                    if type(conf_files) is not list:
                        raise ValueError("manual_file_list Variable is not a list")
                    
                    conf_files = [f.strip() for f in conf_files] # clean up whitespace just in case
                    print("Using manual list from Airflow Variable:", conf_files)
                    return conf_files
                
                except ValueError as ve:
                    print("manual_file_list Variable is not a valid list:", ve)
                    raise AirflowException("Invalid manual_file_list Variable format.")
                
                except Exception as e:
                    print("Failed to parse Airflow Variable manual_file_list:", e)
                    raise AirflowException("Invalid manual_file_list Variable format.")

            if not manual_list:
                print("No manual_file_list Variable set, checking for any month_tag Variable.")
                var_value = Variable.get("epc_month_tag (YYYY-MM)", default=None)
                if var_value:
                    try:
                        parsed = datetime.strptime(var_value, "%Y-%m")
                        month_tag = parsed.strftime("%Y-%m")
                        print(f"Using month tag from Airflow Variable: {month_tag} to fetch file list.")
                        return list_latest_files(month_tag=month_tag)
                    except ValueError:
                        raise ValueError(f"Invalid month tag format in Airflow Variable: {var_value}. Expected 'YYYY-MM'.")

        print("No conf or variable found, fetching latest files from S3.")
        return list_latest_files()

    @task(task_id="check_copy_audit")
    def _check_copy_audit(latest_files: list[str]):
        """Check which of the latest files have not been copied to Snowflake yet."""
        print(f"Latest files to check: {latest_files}")
        if (latest_files is None) or (len(latest_files) == 0):
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
        return copy_and_update([file_path], dag_run_id)

    @task(task_id="trigger_dbt_transformations", trigger_rule="none_failed_min_one_success")
    def _trigger_dbt_transformations(results_list: dict):
        """Merge results from all dynamic copy tasks and decide next action."""

        print("Collected XCom results:", results_list)

        
        combined = {k: v for d in results_list if d for k, v in d.items()}
        print("Merged copy results:", combined)

        if combined is None or len(combined) == 0:
            print("No results found; possibly all skipped.")
            raise AirflowException("No COPY operations were performed.")
        
        not_successes = [k for k, v in combined.items() if v not in ["SUCCESS", "LOADED"]]
        if not_successes:
            print(f"Some files failed or partially loaded: {not_successes}")
            raise AirflowException("One or more COPY operations failed or partially loaded.")
        
        print("All files copied successfully. Ready to trigger downstream DAG.")
        trigger = TriggerDagRunOperator(
            task_id="trigger_dbt_transformations_dag",
            trigger_dag_id="dev_dbt_transformations",
            wait_for_completion=False,
            reset_dag_run=True
        )
        # Context is needed to execute the operator
        context = {}  # empty because this runs within task context
        trigger.execute(context)

    @task(task_id="airflow_var_cleanup")
    def _airflow_var_cleanup():
        """Cleanup Airflow Variables used for ad-hoc runs."""
        try:
            Variable.delete("manual_file_list")
            Variable.delete("epc_month_tag (YYYY-MM)")
            print("Cleaned up Airflow Variables.")
        except Exception as e:
            print(f"Error cleaning up Airflow Variables: {e}")

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
    
    trigger_dbt_transformations = _trigger_dbt_transformations(copy_file)
    airflow_var_cleanup = _airflow_var_cleanup()
    end = _end()

    # DAG sequence, decorator tasks are already linked via above instantiations
    start >> latest_files >> check_copy_audit >> branch_task
    # branching logic is shown here
    branch_task >> skip_copy >> end
    branch_task >> trigger_copy_files >> copy_file >> trigger_dbt_transformations >> airflow_var_cleanup >> end

# Instantiate the DAG
dev_s3_to_snowflake_dag = dev_s3_to_snowflake()