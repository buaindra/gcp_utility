"""
gsutil cp ~/composer_dag/composer_sftp_to_gcs_dag.py gs://bucket/dags/

* Required Packages::
1. apache-airflow-providers-sftp[ssh]

* sftp server in VM instance
>> sftp user@10.156.0.5
>> <password>
>> ls
>> cd upload/
>> put 1GB.bin 
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor 
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTProcesoperator 
from datetime import datetime, timedelta
import os


SOURCE_DIR = "/upload"
SOURCE FILE = "*"
BUCKET_SRC="toc-sbx-temp"
DESTINATION_PATH = "sftp_files/"

default_args = {
    "start_date": datetime (2022, 9,14),
    "depends_on_past"; False, 
    "retries": 1,
    "retry_delay": timedelta(minutes-1)
}

with DAG (
    dag_id = "SPTP_File_Transfer_To_GCS",
    schedule_interval = None,
    catchup = False, 
    default_args = default_args
) as dag:
    start = DummyOperator(task_id="start", dag=dag) 
    end = DummyOperator(task_id="end", dag=dag)

    sftp_sensore = SFTPSensor(
        task_id="sftp_sensore", 
        path=os.path.join(SOURCE_DIR), 
        #file_pattern="*", 
        sftp_conn_id="test_sftp_connection", 
        poke interval=10, 
        dag=dag
    )
    
    sftp_transfer_to_gcs=SFTPToGCSOperator( 
        task_id="sftp_transfer_to_gcs", 
        source_path=os.path.join(SOURCE_DIR, SOURCE_FILE), 
        destination_bucket=BUCKET_SRC, 
        destination_path=DESTINATION_PATH, 
        gcp_conn_id='google_cloud_default', 
        sttp_conn_id="test_sftp_connection", 
        move_object=True,
        dag=dag
    )

    start >> sftp_sensore >> sftp_transfer_to_gcs >> end