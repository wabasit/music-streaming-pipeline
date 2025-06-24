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

    def check_glue_job_status(job_name, task_id, **kwargs):
        ti = kwargs['ti']
        run_id = ti.xcom_pull(task_ids=task_id, key=f"{task_id}_job_run_id")
        if not run_id:
            raise ValueError(f"No JobRunId found in XCom for {task_id}")
        glue = boto3.client('glue', region_name=region)
        max_attempts = 20
        attempt = 0
        while attempt < max_attempts:
            status = glue.get_job_run(JobName=job_name, RunId=run_id)['JobRun']['JobRunState']
            print(f"{job_name} status: {status}")
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                if status != 'SUCCEEDED':
                    error_msg = f"{job_name} failed with status {status}"
                    boto3.client('sns', region_name=region).publish(TopicArn=sns_arn, Message=error_msg, Subject="Airflow Failure")
                return 'run_transform_job' if status == 'SUCCEEDED' else 'notify_failure'
            time.sleep(30)
            attempt += 1
        raise TimeoutError(f"Glue job {job_name} status check timed out")

    run_validation_job = PythonOperator(
        task_id="run_validation_job",
        python_callable=lambda **kwargs: run_glue_job(validation_job, "run_validation_job", **kwargs),
        provide_context=True,
    )

    check_validation_status = BranchPythonOperator(
        task_id="check_validation_status",
        python_callable=lambda **kwargs: check_glue_job_status(validation_job, "run_validation_job", **kwargs),
        provide_context=True,
    )

    run_transform_job = PythonOperator(
        task_id="run_transform_job",
        python_callable=lambda **kwargs: run_glue_job(glue_job_name, "run_transform_job", **kwargs),
        provide_context=True,
    )

    check_transform_status = BranchPythonOperator(
        task_id="check_transform_status",
        python_callable=lambda **kwargs: check_glue_job_status(glue_job_name, "run_transform_job", **kwargs),
        provide_context=True,
    )

    def run_dynamodb_loader():
        # response = glue.start_job_run(JobName=dynamodb_job_name)
        response = glue.start_job_run(JobName="load_dynamodb")
        print(f"Started DynamoDB Glue job: {response['JobRunId']}")

    load_to_dynamodb = PythonOperator(
        task_id="load_to_dynamodb",
        python_callable=run_dynamodb_loader,
    )

    def archive_streamed_files():
        today = datetime.utcnow().strftime("%Y-%m-%d")
        archive_path = f"{streaming_prefix}/archived/{today}/"
        bucket_obj = s3.Bucket(bucket)
        for obj in bucket_obj.objects.filter(Prefix=f"{streaming_prefix}/"):
            if "archived" in obj.key:
                continue
            s3.Object(bucket, f"{archive_path}{obj.key.split('/')[-1]}").copy({'Bucket': bucket, 'Key': obj.key})
            s3.Object(bucket, obj.key).delete()

    archive_data = PythonOperator(
        task_id="archive_streamed_data",
        python_callable=archive_streamed_files,
    )

    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn=sns_arn,
        message="Music Pipeline completed successfully.",
        subject="Airflow Success",
        aws_conn_id="aws_default",
    )

    notify_failure = SnsPublishOperator(
        task_id="notify_failure",
        target_arn=sns_arn,
        message="Music Pipeline failed. Check logs for details.",
        subject="Airflow Failure",
        aws_conn_id="aws_default",
    )