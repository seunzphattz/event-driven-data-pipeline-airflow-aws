from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import boto3
import logging
import time

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # Use a fixed start_date (best practice). Avoid datetime.now() in DAG definition.
    "start_date": datetime(2026, 2, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

dag = DAG(
    dag_id="process-songs-metrics",
    default_args=default_args,
    description="Trigger Glue job when new files are uploaded to S3 and manage output",
    schedule="*/5 * * * *",
    catchup=False,
)

BUCKET_NAME = "spotify-da"
REGION = "us-east-1"


def check_files_in_s3(prefix: str) -> bool:
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    contents = response.get("Contents", [])
    logging.info("Contents in %s: %s", prefix, contents)

    for obj in contents:
        if obj.get("Size", 0) > 0:
            logging.info("Non-empty file found in %s: %s", prefix, obj.get("Key"))
            return True

    logging.info("No non-empty files found in %s", prefix)
    return False


def check_all_files(**context):
    logging.info("Checking for files in S3 prefixes")
    user_streams = check_files_in_s3("spotify-da1/user-streams/")
    songs = check_files_in_s3("spotify-da1/songs/")
    users = check_files_in_s3("spotify-da1/users/")

    logging.info("user_streams=%s songs=%s users=%s", user_streams, songs, users)

    if user_streams and songs and users:
        logging.info("All directories have files, proceeding with Spark job")
        return "trigger_spark_job_task"
    else:
        logging.info("One or more directories are missing files, skipping execution")
        return "skip_execution"


def wait_for_glue_job_completion(job_name: str, client, poll_interval: int = 60):
    while True:
        response = client.get_job_runs(JobName=job_name, MaxResults=1)
        job_runs = response.get("JobRuns", [])

        if job_runs and job_runs[0].get("JobRunState") in ["RUNNING", "STARTING", "STOPPING"]:
            logging.info("Glue job %s is still running. Waiting...", job_name)
            time.sleep(poll_interval)
        else:
            logging.info("Glue job %s has finished (or no runs found).", job_name)
            break


def trigger_glue_job(job_name: str, **context):
    client = boto3.client("glue", region_name=REGION)
    logging.info("Checking if Glue job %s is running...", job_name)
    wait_for_glue_job_completion(job_name, client)

    logging.info("Triggering Glue job: %s", job_name)
    client.start_job_run(JobName=job_name)


def wait_for_job(job_name: str, **context):
    client = boto3.client("glue", region_name=REGION)
    wait_for_glue_job_completion(job_name, client)


def move_files_to_archived(**context):
    s3 = boto3.client("s3")
    source_prefix = "spotify_data/user-streams/"
    dest_prefix = "spotify_data/user-streams-archived/"

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=source_prefix)
    for obj in response.get("Contents", []):
        source_key = obj["Key"]
        dest_key = source_key.replace(source_prefix, dest_prefix)

        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={"Bucket": BUCKET_NAME, "Key": source_key},
            Key=dest_key,
        )
        s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)


check_files = BranchPythonOperator(
    task_id="check_files",
    python_callable=check_all_files,
    dag=dag,
)

trigger_spark_job_task = PythonOperator(
    task_id="trigger_spark_job_task",
    python_callable=trigger_glue_job,
    op_args=["calculate_metrics_etl"],
    dag=dag,
)

wait_for_spark_job_completion_task = PythonOperator(
    task_id="wait_for_spark_job_completion_task",
    python_callable=wait_for_job,
    op_args=["calculate_metrics_etl"],
    dag=dag,
)

trigger_python_job_task = PythonOperator(
    task_id="trigger_python_job_task",
    python_callable=trigger_glue_job,
    op_args=["insert_metrics_dynamo"],
    dag=dag,
)

wait_for_python_job_completion_task = PythonOperator(
    task_id="wait_for_python_job_completion_task",
    python_callable=wait_for_job,
    op_args=["insert_metrics_dynamo"],
    dag=dag,
)

move_files = PythonOperator(
    task_id="move_files",
    python_callable=move_files_to_archived,
    dag=dag,
)

skip_execution = EmptyOperator(
    task_id="skip_execution",
    dag=dag,
)

# Dependencies
check_files >> [trigger_spark_job_task, skip_execution]
trigger_spark_job_task >> wait_for_spark_job_completion_task >> trigger_python_job_task >> wait_for_python_job_completion_task >> move_files
