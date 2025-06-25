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
    schedule_interval=timedelta(minutes=15),  # Every 15 minutes
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

    def check_glue_job_status(job_name, task_id, next_task_on_success, **kwargs):
        ti = kwargs['ti']
        run_id = ti.xcom_pull(task_ids=task_id, key=f"{task_id}_job_run_id")
        if not run_id:
            raise ValueError(f"No JobRunId found in XCom for {task_id}")
        glue = boto3.client('glue', region_name=region)
        max_attempts = 40  # Increased timeout for longer jobs
        attempt = 0
        while attempt < max_attempts:
            status = glue.get_job_run(JobName=job_name, RunId=run_id)['JobRun']['JobRunState']
            print(f"{job_name} status: {status}")
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                if status != 'SUCCEEDED':
                    error_msg = f"{job_name} failed with status {status}"
                    boto3.client('sns', region_name=region).publish(TopicArn=sns_arn, Message=error_msg, Subject="Airflow Failure")
                    return 'notify_failure'
                return next_task_on_success
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
        python_callable=lambda **kwargs: check_glue_job_status(validation_job, "run_validation_job", "run_transform_job", **kwargs),
        provide_context=True,
    )

    run_transform_job = PythonOperator(
        task_id="run_transform_job",
        python_callable=lambda **kwargs: run_glue_job(glue_job_name, "run_transform_job", **kwargs),
        provide_context=True,
    )

    check_transform_status = BranchPythonOperator(
        task_id="check_transform_status",
        python_callable=lambda **kwargs: check_glue_job_status(glue_job_name, "run_transform_job", "run_dynamodb_job", **kwargs),
        provide_context=True,
    )

    # Fixed DynamoDB loader to wait for completion
    def run_and_wait_dynamodb_job(**kwargs):
        glue = boto3.client('glue', region_name=region)
        sns = boto3.client('sns', region_name=region)
        
        # Check for active runs first
        response = glue.get_job_runs(JobName=dynamodb_job_name, MaxResults=10)
        active_runs = [r for r in response['JobRuns'] if r['JobRunState'] in ['RUNNING', 'STARTING', 'QUEUED']]
        if active_runs:
            print(f"DynamoDB job already running. Active runs: {len(active_runs)}")
            # Wait for existing run to complete or fail
            run_id = active_runs[0]['Id']
        else:
            # Start new job
            response = glue.start_job_run(JobName=dynamodb_job_name)
            run_id = response['JobRunId']
            print(f"Started DynamoDB Glue job: {run_id}")
        
        # Store run_id in XCom for potential debugging
        kwargs['ti'].xcom_push(key="dynamodb_job_run_id", value=run_id)
        
        # Wait for completion
        max_attempts = 60  # 30 minutes timeout
        attempt = 0
        while attempt < max_attempts:
            status = glue.get_job_run(JobName=dynamodb_job_name, RunId=run_id)['JobRun']['JobRunState']
            print(f"DynamoDB job status: {status}")
            
            if status == 'SUCCEEDED':
                print("DynamoDB job completed successfully")
                return
            elif status in ['FAILED', 'STOPPED']:
                error_msg = f"DynamoDB job failed with status {status}"
                sns.publish(TopicArn=sns_arn, Message=error_msg, Subject="DynamoDB Load Failed")
                raise RuntimeError(error_msg)
            
            time.sleep(30)
            attempt += 1
        
        # Timeout
        error_msg = f"DynamoDB job timed out after {max_attempts * 30} seconds"
        sns.publish(TopicArn=sns_arn, Message=error_msg, Subject="DynamoDB Load Timeout")
        raise TimeoutError(error_msg)

    run_dynamodb_job = PythonOperator(
        task_id="run_dynamodb_job",
        python_callable=run_and_wait_dynamodb_job,
        provide_context=True,
    )

    def archive_streamed_files():
        today = datetime.utcnow().strftime("%Y-%m-%d")
        archive_path = f"{streaming_prefix}archived/{today}/"  # Fixed path
        bucket_obj = s3.Bucket(bucket)
        
        files_to_archive = []
        for obj in bucket_obj.objects.filter(Prefix=f"{streaming_prefix}"):
            if "archived" not in obj.key and obj.key.endswith('.csv'):
                files_to_archive.append(obj.key)
        
        print(f"Archiving {len(files_to_archive)} files")
        for key in files_to_archive:
            filename = key.split('/')[-1]
            archive_key = f"{archive_path}{filename}"
            s3.Object(bucket, archive_key).copy({'Bucket': bucket, 'Key': key})
            s3.Object(bucket, key).delete()
            print(f"Archived {key} to {archive_key}")

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

    end = DummyOperator(task_id="end")

    # === FIXED DAG Flow ===
    start >> wait_for_stream_file >> run_validation_job >> check_validation_status
    
    # Validation branch
    check_validation_status >> run_transform_job >> check_transform_status
    check_validation_status >> notify_failure
    
    # Transform branch  
    check_transform_status >> run_dynamodb_job >> archive_data >> notify_success >> end
    check_transform_status >> notify_failure
    
    # Failure path
    notify_failure >> end