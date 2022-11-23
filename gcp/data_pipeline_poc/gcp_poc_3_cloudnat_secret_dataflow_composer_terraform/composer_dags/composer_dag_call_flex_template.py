"""
License: Free to use
"""

# import required packages/modules

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.google.cloud.operators.dataflow Import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator 
from airflow.providers.google.cloud.operators.bigquery Import (BigQueryInsertJobOperator)
from airflow.utils.task group import TaskGroup
from airflow.models.param Import Param from airflow.models import Variable
import google.cloud.logging from google.cloud.logging_v2.resource import Resource
from google.cloud import bigquery datetime import datetime, timedelta
import os
import json

"""
**** Composer DAG Description:
 * DAG to call the generic dataflow flex template to request multiple dataflow jobs for multiple apis.
 * Then it will fetch the row count from bigquery for further data pipeline monitoring.

**** DAG will access 4 required composer environment variable:
 * 1. project id
 * 2. region
 * 3. subnetwork
 * 4. service account email

**** DAG will access 1 required airflow variable: 
 * 1. vars json: provide dataflow flex template location, dataflow temp location, 
 api endpoint url, bigquery sink table details and secret id for the api key.

 * sample of varsson:
 * vars json = { 
        "dataflow_template": "gs://<bucket>/dataflow/pipelines/api-to-bigquery-icon-ex/template/ap1_to_bigquery_json_flex.json",
        "dataflow_temp_location": "gs://<bucket/dataflow/pipelines/api-to-bigquery-json-flex/temp",
        apis: [
            {
                "api name": "ebird", 
                "param_input_api_endpoint": "https://api.ebird.org/v2/data/obs/CA/recant",
                "param_output_bq_table": "<project_id>:beam_dataset.ebird_api_json_table", 
                "secret_id_api_key": "<project_id>:X-eBirdAoiToken:dataflow-api-key"
            },
            {
                "api name": "animal", 
                "param_input_api_endpoint": "https://zoo-animal-api.herokuapp.com/animals/rand/5",
                "param_output_bq_table": "<project_id>:beam_dataset.animal_api_json_table"
            },
            {
                "api name": "apiendpoint", 
                "param_input_api_endpoint": "https://api.ipify.org?format=json",
                "param_output_bq_table": "<project_id>:beam_dataset.endpoint_api_json_table"
            }
        ]
    }
    
"""

# Environment variables
project_id = os.environ.get("project_id", "")
region = os.environ.get("region", "")
subnetwork = os.environ.get("subnetwork","") 
service_account_email = os.environ.get("service_account_email", "")  #test-sa@<project_id>:iam.gserviceaccount.com"


# Airflow variables
param_vars_json = Variable.get("vars_json", deserialize json-True) 
dataflow_template = "{{ var.json.vars_json.dataflow_template }}" 
dataflow_temp_location = "{{ var.json.vars_json.dataflow_temp_location }}" 
api_list = param_vars_json.get("apis", [])

logger_name = "composer-api-to-bigquery-json-flex"
log_resource = Resource(type='global',
                        labels={"dag Id": "composer-api-to-bigquery-json-pipeline"})


def _custom_log(logger_name): 
    gcloud_logging_client = google.cloud.logging.Client()
    logger = gcloud_logging client.logger(logger_name) return logger
    return logger

   
logger = _custom_log(logger_name)


#default arguments 
default_args = {
    "start_date": datetime(2022, 8, 16),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes-3),
    #"dataflow_default_options": {
    #    "numWorkers": dataflow_numworkers, 
    #    "maxWorkers": dataflow_maxworkers,
    #    "serviceAccountEmail": dataflow_sa,
    #    "templocation": dataflow_temp_location,
    #    "workerRegion": region
    # }
}
I

def _display_var(**kwargs): 
    logger-log struct({
        "func_display_var": "python function display var called from composer dag", 
    }, severity="INFO", resource=log_resource)

    for key, val in kwargs.items(): 
    # print(key: (key), val: (val)")
    logger.log_test(f"key: {key}, val: {val}", severity="INFO")


def _check_bq_row_count(**kwargs): 
    bq_client = bigquery.Client()
    table = kwargs.get("table_name", "")
    table_name = table.replace(":", ".") 
    query = (f"select count(1) as count_row from `{table_name}`") 
    query_job = bq_client.query(query) 
    rows = query_job.result()

    row_count=0
    for row in rows: 
        row_count = row_count + int(row.count_row)

    logger.log_text(f"table {table_name} has total {row_count} records.", severity="INFO", resource=log_resource)
    logger.log struct({
        "table_name: table_name, 
    }, severity="INFO", resource=log_resource)


#composer dag
with DAG(
    dag_id="composer_api_to_bigquery_json", 
    schedule_interval=None,
    tags=['api_to_bigquery_json'),
    default_args=default_args,
    catchup=False
)as dag:

    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag, trigger_rule="none_failed_or_skipped")

    display_var = PythonOperator(
        task_id = "display_van",
        provide_context=True,
        python_callable = _display_var, 
        op_kwargs={"vars_json": param_vars_json, "project_id": project_id, "region": region},
        dag = dag
    )

    if api_list:
        for api in api_list:
            with TaskGroup(group_id=f"taskgroup_{api.get('api_name', '')}") as api_taskgroup:

                ROWS_COUNT_QUERY = (
                    f"SELECT COUNT(1) as row_count FROM {api.get('param_output_bq_table', '').split(':')[-1]}"
                )

                t1 = DummyOperator(task_id=f"Dummy_{api.get('api_name', '')}", dag=dag)

                flex_template_api_to_bq = DataflowStartFlexTemplateOperator(
                    task_id=f"flex_template_api_to_bq_{api.get('api_name', '')}", 
                    body={
                        "launchParameter": {
                            "containerSpecGcsPath": dataflow_template,
                            "jobName": f"composer-flex-api-to-bq-json-{api.get('api_name', '')}-"+"{{ ds_nodash }}",
                            "parameters": {
                                "input_api_endpoint": f"{api.get('param_input_api_endpoint', '')}", 
                                "output_bq_table": f"{api.get('param_output_bq_table', '')}",
                                "secret_id_api_key": f"{api.get('secret_id_api_key', None)}"
                            },
                            "environment": {
                                "serviceAccountEmail": service_account_email,
                                "subnetwork": f"https://www.googleapis.com/compute/v1/projects/(project_id)/regions/(region)/subnetworks/{subnetwork)",
                                "ipConfiguration": "WORKER_IP_PRIVATE"
                            },
                        }
                    },
                    do_xcom_push=False,
                    wait_until_finished=True,
                    project_id-project_id,
                    location=region
                )

                check_bq_row_count = PythonOperator(
                    task_id=f"check_bq_row_count_{api.get('api_name', '')}",
                    provide_context=True,
                    python_callable=_check_bq_row_count,
                    op_kwargs={"table_name": f"{api.get('param_output_bq_table', '')}"} # "{{ task_instance_key_str }}"
                )

                display_var >> t1 >> flex_template_api_to_bq >> check_bq_row_count >> end


                ## dataflow classic template operator
                #dataflow_template_api_to_bq DataflowTemplated JobStartOperator(
                #   task_id="dataflow template_api_to_bq",
                #   template=dataflow_template,
                #   job_name="composer-api-to-bigquery-json-{{ ds_nodash}}", 
                #   project_id=project_id,
                #   location=region,
                #   parameters={
                #       "input_api_endpoint": param_input_api_endpoint,
                #       "output_bq_table": param_output_bq_table,
                #   },
                #   wait_until_finished=True,
                #   dag=dag,
                #)

                #start >> display var >> dataflow_template_api_to_bq >> end

                start >> display_var >> api_taskgroup >> end