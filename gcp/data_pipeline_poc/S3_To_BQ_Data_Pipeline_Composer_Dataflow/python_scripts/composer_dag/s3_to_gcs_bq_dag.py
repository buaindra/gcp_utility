#Importing Python Libraries

import os
from datetime import datetime, timedelta
from airflow import models
from airflow import DAG
from airflow.operators import bash_operator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#from airflow.utils import trigger_rule


yesterday = datetime.today() - timedelta(days=1)
#start_date = datetime(2021,12,15,13,30,00)

#Specify the default_args
default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": yesterday,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

#GCP_PROJECTID=os.environment.get("GCP_PROJECTID", "test-poc01-330806")
#GCS_BUCKET=os.environment.get("GCP_BUCKET")
#AWS_BUCKET=os.environment.get("AWS_BUCKET")
#AWS_FILE_PREFIX=os.environment.get("AWS_FILE_PREFIX")
#BQ_DATASET=os.environment.get("BQ_DATASET")
#BQ_TABLE=os.environment.get("BQ_TABLE")


GCP_PROJECTID="indranil-24011994-04"
GCS_BUCKET="indranil-24011994-04"
AWS_BUCKET="s3-bucket-test-0001"
AWS_FILE_PREFIX="National_Stock_Exchange_of_India_Ltd"
BQ_DATASET="poc_composer_dataflow"
BQ_TABLE="comp_stock_details"
GCS_PYTHON="gs://indranil-24011994-04/gcs_to_bq_beam_batch.py"
GCS_OUTPUT="gs://indranil-24011994-04/output"

#Initiate the DAG
with models.DAG (
    dag_id = "bq_composer_poc_dag",
    default_args = default_args,
    schedule_interval = timedelta(days=1)
    #schedule_interval = "@daily"
) as dag:
    
    wait_dag = bash_operator.BashOperator(
        task_id = "wait_dag",
        bash_command = "sleep 5s"
    )
    
    print_dag_starttime = bash_operator.BashOperator(
        task_id = "print_dag_starttime",
        bash_command = "echo S3 to GCS data transfer started @ $(date)"
    )
    
    s3_to_gcs = S3ToGCSOperator(
        task_id = "s3_to_gcs",
        bucket = AWS_BUCKET,
        prefix = AWS_FILE_PREFIX,
        aws_conn_id = 'aws_default',
        verify = False,
        dest_gcs = "gs://" + GCS_BUCKET + "/input/"
    )
    
    comp_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=GCS_BUCKET + "/input/",
        source_objects=['*'],
        destination_project_dataset_table=GCP_PROJECTID+"."+BQ_DATASET+"."+BQ_TABLE,
        skip_leading_rows=1,
        #write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        source_format='CSV'
    )

    beam_gcs_to_bq = BeamRunPythonPipelineOperator(
    task_id="beam_gcs_to_bq",
    runner="DataflowRunner",
    py_file=GCS_PYTHON,
    py_options=[],
    pipeline_options={
        'output': GCS_OUTPUT,
    },
    py_requirements=['apache-beam[gcp]==2.34.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config={
        "job_name": "start-python-job-async",
        "location": 'us-central1',
        "wait_until_finished": False,
    },
    )

    print_dag_endtime = bash_operator.BashOperator(
        task_id = "print_dag_endtime",
        bash_command = "echo S3 to GCS data transfer ended @ $(date)"
    )
    
    wait_dag >> print_dag_starttime >> s3_to_gcs >> [comp_gcs_to_bq, beam_gcs_to_bq] >> print_dag_endtime
    
    
    
    
    
    
    