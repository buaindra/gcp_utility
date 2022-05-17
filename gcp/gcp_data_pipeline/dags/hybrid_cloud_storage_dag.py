import datetime
from airflow import models
from airflow.operators.dummy import DummyOperator
# from airflow.operators import bash_operator
# from airflow.operators import python_operator

# libraries required for data transfer from Azure
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor  # WasbPrefixSensor
from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import AzureBlobStorageToGCSOperator


# set variables
AZURE_CONTAINER_NAME = "newcontainer"
AZURE_BLOB_NAME = "results-20210417-001900.csv"
GCP_BUCKET_NAME = "coherent-coder-346704"
GCP_BUCKET_FILE_PATH = "results-20210417-001900.csv"
GCP_OBJECT_NAME = "results-20210417-001900.csv"

# set default dag args
default_dag_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	"start_date": datetime.datetime.today(),
	'email': ['airflow@example.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': datetime.timedelta(minutes=5)
}


with models.DAG(
	"data_transfer_to_gcp",
	schedule_interval=None,	 # datetime.timedelta(days=1)
	default_args= default_dag_args,
    catchup=False
    ) as dag:


	# Transfer Data from Azure Blob Storage to Google Cloud Storage
	start = DummyOperator(
		task_id = "start",
		dag = dag
	)

	# Transfer Data from Azure Blob Storage to Google Cloud Storage
	wait_for_azure_blob = WasbBlobSensor(
		task_id="wait_for_azure_blob",
		container_name=AZURE_CONTAINER_NAME,
		blob_name=AZURE_BLOB_NAME,
		wasb_conn_id='wasb_default',
		dag = dag
	)

	transfer_files_azure_blob_to_gcs = AzureBlobStorageToGCSOperator(
		task_id="transfer_files_azure_blob_to_gcs",
		# AZURE arg
		container_name=AZURE_CONTAINER_NAME,
		blob_name=AZURE_BLOB_NAME,
		file_path=GCP_OBJECT_NAME,
		# GCP args
		bucket_name=GCP_BUCKET_NAME,
		object_name=GCP_OBJECT_NAME,
		filename=GCP_BUCKET_FILE_PATH,
		gzip=False,
		delegate_to=None,
		impersonation_chain=None,
		dag = dag
	)

	end = DummyOperator(
		task_id = "end",
		dag = dag
	)

	start >> wait_for_azure_blob >> transfer_files_azure_blob_to_gcs >> end