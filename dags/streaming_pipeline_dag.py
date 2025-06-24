from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from datetime import datetime
import boto3
import time
from datetime import timedelta

# AWS Clients
region = "eu-north-1"
glue = boto3.client("glue", region_name=region)
s3 = boto3.resource("s3", region_name=region)

# Variables
glue_job_name = Variable.get("GLUE_JOB_NAME")
validation_job = "val_glue_job"
dynamodb_job_name = "load_dynamodb"
bucket = Variable.get("S3_BUCKET")
streaming_prefix = Variable.get("S3_STREAMING_PREFIX").replace("s3://project3dt/", "")
sns_arn = Variable.get("SNS_TOPIC_ARN")

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id="music_data_pipeline",
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=15),  # Every 5 minutes
    catchup=False,
    default_args=default_args
) as dag:

    start = DummyOperator(task_id="start")

    wait_for_stream_file = S3KeySensor(
        task_id="await_stream_files",
        bucket_name=bucket,
        bucket_key=f"{streaming_prefix}*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=60 * 60,
        poke_interval=120,
        mode="reschedule",
    )

    def run_glue_job(job_name, task_id, **kwargs):
        glue = boto3.client('glue', region_name=region)
        sns = boto3.client('sns', region_name=region)
        max_attempts = 3
        attempt = 1
        while attempt <= max_attempts:
            response = glue.get_job_runs(JobName=job_name, MaxResults=10)
            active_runs = [r for r in response['JobRuns'] if r['JobRunState'] in ['RUNNING', 'STARTING', 'QUEUED']]
            if active_runs:
                if attempt == max_attempts:
                    sns.publish(TopicArn=sns_arn, Message=f"{job_name} failed to start. Active runs present.", Subject="Airflow Failure")
                    raise RuntimeError(f"Too many active runs for {job_name}")
                time.sleep(30)
                attempt += 1
                continue
            run_id = glue.start_job_run(JobName=job_name)['JobRunId']
            kwargs['ti'].xcom_push(key=f"{task_id}_job_run_id", value=run_id)
            print(f"Started {job_name} with run ID {run_id}")
            return

    