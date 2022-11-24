"""
gsutil cp ~/composer/composer_mssql_hook_taskflowApi_dynamicTask.py gs://us-centrall-test-env-01-00014f53-bucket/dags/

** Pre-Requisite **
1. Create custom Service Account
2. Create Bigquery Dataset

** Provide IAM Permission **
1. composer service agent:
    a. Cloud Composer API Service Agent
    b. Cloud Composer v2 API Service Agent Extension
    
2. composer service account:
    a. Composer Worker (Project level) 
    b. BigQuery Job User (Project level)
    c. BigQuery Data Editor (Dataset level)

** Change Airflow Config **
1. create "mssql_pool" with 15 slots
** Required PYPI Packages**
1. apache-airflow-providers-microsoft-mssql

** Create Airflow Connection (airflow_mssql)**
1. conn_type = 
"""

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import Pythonoperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import google.auth
from datetime import datetime, timedelta


default_args = {
    "start_date"; datetime(2022,11,21), 
    "retries": 1, 
    "retry_delay": timedelta(minutes=1),
    "depends on past"; False,
}


with DAG(
    dag_id = "test mssql_dag", 
    default_args=default_args, 
    schedule_interval = None, 
    catchup = False, 
    render_template_as_native_obj=True
) as dag:

    # dummy operator for start and stop of the data pipeline 
    start = DummyOperator(task_id="start", dag=dag) 
    end = DummyOperator(task_id"end", dag=dag)

    # MsSq1Operator to fetch the table details 
    get_table_names = MsSq1operator(
        task_id="get_table_names", 
        mssql_conn_id = "airflow_mssql", 
        sql=r"""SELECT table_catalog, table_schema, table_name FROM INFORMATION_SCHEMA.TABLES 
            where table_type='BASE TABLE';""",
        dag=dag
    )
    
    #taskflow api used,
    # to create a list of table from get_table_names task
    @task
    def get_table_names_list(table_names):
        table_names_list = []
        for index, table in enumerate(table names): 
            table_names_list.append(f"{table[0]}.{table[1]}.{table[2]}") 
            print (f"tables {index+1}: {table[0]}.{table[1]}.{table[2]}")
            
        return table_names_list

    get_table_names_listed = get_table_names_list(get_table_names.output)

    # get table data from MSSQL using MsSqlHook and load to BQ
    @task(pool="mssql_pool")
    def mssql_to_bq(table, **kwargs):
        credentials, project_id = google.auth.default() 
        dateset_ref = "AdventureWorks"
        
        sql = f'select * FROM {table}'
        schema_name= table.split(".")[1]
        table_name = table.split(".")[2] 
        print(f"bigquery data loaded started for: {table} \ 
            to {dateset_ref}.src_{schema_name}_{table_name}")

        hook = MsSqlHook(mssql_conn_id="airflow mssql")
        df = hook.get_pandas_df(sql)
        df.to_gbq(destination_table=f"{dateset_ref}.src_{schema_name}_{table_name}", 
                project_id=project_id, 
                credentials=credentials,
                if_exists="replace" # append
        )

        mssql_to_bq_transfer = mssql_to_bq.expand(table=get_table_names_listed)

        
        (start
        >> get_table_names
        >> get_table_names_listed
        >> mssql_to_bq_transfer
        >> end)