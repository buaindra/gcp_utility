from airflow import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator 
from datetime import datetime, timedelta 
import os

from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils 
import google.auth.transport.requests 
from google.auth.transport.requests import AuthorizedSession

#cloud_function_id = "test-function-trigger" 
#project_id = "sappi-toc-sbx" 
#region = "europe-west1" 
cf_url = "https://test-function-trigger-7c45zirlsa-ew.a.run.app"


default_args = {
    "start_date": datetime(2022, 9, 14), 
    "depends_on_past" : False, 
    "retries": -1,
    "retry_delay": timedelta(minutes=1)

def invoke_cloud_function(**kwargs): 
    url = str(kwargs.get("ct_url", "No url")) 
    print(f"url: {url}")

    #this is a request for obtaining the the credentials 
    request = google.auth.transport.requests.Request()
    id_token credentials = id_token_credential_utils.get_default_id token credentials(url, request=request)
    # the authorized session object is used to access the Cloud Function
    resp = AuthorizedSession(id_token_credentials).request("POST", url=uri, params=("name": "Indranil Pal"))

    print(resp.status_code) # should return 200 
    print(resp.content) # the body of the HTTP response


with DAG(
    dag_id = "Calling_CF_DAG", 
    schedule_interval = None, 
    catchup = False, 
    default_args = default_args
)as dag:
    start = DummyOperator(task_id="start", dag=dag) 
    end = DummyOperator(task_id="end", dag=dag)

    #call_cf = CloudFunctionInvokeFunctionOperator(
    #    task_id = "call_cf",
    #    function_id = cloud_function_id,
    #    input_data = {"name": "Indranl1"},
    #    Location = region.
    #    project id= project id,
    #    #impersonation_chain=["5260000000-compute@developer.gserviceaccount.com", "service-composer-demo@toc-sbx.iam.gserviceaccount.com".
    #    gep_conn_id='cf_http_connection",
    #    dag = dag
    #)    

    invoke_cf = PythonOperator( 
        task_id="invoke_cf", 
        python_callable=invoke_cloud_function,
        provide_context=True,
        op_kwargs={"cf_url": cf_url},
        dag=dag
    )

    start >> invoke_cf >> end