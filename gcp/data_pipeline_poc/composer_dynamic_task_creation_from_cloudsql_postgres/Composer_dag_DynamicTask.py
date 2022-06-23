'''
gsutil cp/working/dynamic_task_group_dag.py gs://<bucket>/dags/
gsutil cp/working/ConfigFile.properties gs://<bucket>/data/
'''

import string
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.contrib.operators.dataproc_operator import DataprocclusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator
from airflow.utils.task_group import TaskGroup
from airflow import models
from datetime import datetime, timedelta
from airflow.models.param import Param  #added this to use Param object
from airflow.models import Variable  #added this to use Variable
import logging  #added this to use Logging INFO
import configparser
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud import bigquery  # needs to install pypi
import uuid
#import os
#import numpy as np  #for np. fromiter

project_id=models.Variable.get("project_id"))
region=models.Variable.get("region")

env "DEV
config=confignarser.ConfigParser()
config.read(/home/airflow/ges/data/ConfigFile.properties")
default_tables=config.get(env, "tables_name").split(", ")[:1]
Logging.info(f"{default_tables}")
batch_id = str(uuid.uuid4())


# hard coded table

# default_tables = []
# for i in range (1,11):
#     table_name = "table_" + str(i) 
#     default_tables.append(table_name)

def _custom_log(logger_name):
    gcloud_logging_client = google.cloud.logging.Client()

    # gcloud_logging_handler = CloudLoggingHandler(gcloud_logging client, name=logger_name)
    # stream handler = logging. StreamHandler() stream_handler.setLevel (logging. WARNING)
    logger = gcloud_logging_client.logger(logger_name) 
    # logger = logging.getLogger(logger_name)
    # Logger.setLevel (logging.DEBUG)
    # logger.addHandler(gcloud_logging_handler) 
    # logger.addHandler(stream_handler)
    return logger

logger = _custom_log (logger_name)
#custom_logger.info("sample_logging_test dag logging_test")
logger.log_text("sample_logging_test_dag_logging_test", severity="INFO") 
logger.log_struct({
    "name": "King Arthur",
})

default_args = {
    "start_date': datetime (2022, 3, 1), #example date
    "project_id": project_id,
    "region": region,
    "depends_on_past" : False,
    "retries": 1,
    "retry_delay": timedelta (minutes=3),
}

##Please replace the function with your cloudsql config table details. ##This function will read the config table and return the table names t def read_config_db(**kwargs):
#SRC TBL NM = kwargs.get('SRC_TBL_NM")

def read_config_db(**kwargs):
    #SRC_TBL_NM = kwargs.get("SRC_TBL_NM", [])
    SRC_TBL_NM = kwargs["dag_run"].conf.get("SRC_TBL_NM", "ALL")
    logging.info("Current SRC TBL NM value is " + str(SRC_TBL_NM))

    where additional = ""
    if (SRC TBL NM != "ALL" and type (SRC TBL NM) == list): 
        for i in SRC_TBL_NM:
            where_additional = where_additional + "'" + str(i) + "', "
        where_additional = "where table name in (" + where additional[:-2] + ")"

    query = "SELECT table_name FROM config_db" + where_additional + ";" 
    #query = "SELECT table_name FROM config_db;"
    logging.info("Current query value is " + str(query))

    postgres=PostgresHook("postgres_default")
    conn = postgres.get_conn() 
    cursor = conn.cursor()
    cursor.execute(query)
    table_names = [i[0] for i in cursor.fetchall()] 
    logging.info("Return result set value is " + str(table_names))

    ti = kwargs['ti'] 
    # results = []
    # if kwargs["dag_run"].conf.get("SRC_TBL_NM", "") == "ALL":
        # results.extend(default_tables)
    # else:
        # results.extend(kwargs["dag_run"].conf.get("SRC_TBL_NM", []))
    #ti.xcom_push(key= 'FINAL_SRC_TBL_NM', value=results) 
    # Logging.info(f"{results = }") 
    ti.xcom_push(key='FINAL_SRC_TBL_NM', value-table_names)


def branching_func(**kwargs):
    ti = kwargs['ti']
    executable_tasks=ti.xcom_pull(key="FINAL_SRC_TBL_NM", task_ids="read_con 
    #new_List = ["tkl "+str(table) for table in executable tasks] 
    new_list = [str(table)+"_TaskGrp.tk1 "+str(table) for table in executable_tasks] 
    logging.info(f"executable_tasks (new_list =}") 
    return new_list


#this function will return the table list 5
def return_config():
    FINAL_SRC_TBL_NM = default_tables 
    logging.info("Current FINAL SRC TBL NM value is " + str(FINAL_SRC_TBL_NM))
    return FINAL_SRC_TBL_NM


with DAG(
    dag_id="dag_dynamic_task", 
    schedule interval=None,
    tags=['ingestion'],
    default_args=default_args, 
    template_searchPath=["/home/airflow/gcs/dags/"],  # required for bigquery operator
    catchup=False,
    params={ 
        'SRC_TBL_NM': Param("ALL", type=['string', 'array']), # default ALL and use json schema validata
    },
) as dag:

    #Dummy DAGS
    start=DummyOperator(task_id="start", dag=dag) 
    end=DummyOperator(task_id="end", dag=dag, trigger_rule="none_failed_or_skipped")

    read_config_db = PythonOperator(
        task_id='read_config_db,
        dag=dag,
        provide_context=True,
        python_callable=read_config_db,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True, 
        python_callable=_branching_func,
        do_xcom_push-False,
        dag=dag
    )
    
    # requiered parameter missing error occured
    # merge_query_job = BigQueryInsertJobOperator(
    #     task_id="merge_query_job",
    #     configuration={
    #         "query": {
    #               # "query": "{% include 'sql/merge_statement.sql' %}",  # sql/ is folder created inside dags folder
    #               "query": "insert into `gls-customer-poc.ds2.source_staging`  \
    #                         select * from `gls-customer-poc.ds2.source`;",
    #               "useLegacySql": False,
    #               "writeDisposition": "WRITE TRUNCATE",
    #               "createDisposition": "CREATE IF NEEDED",
    #               "destinationTable": {"datasetId": "ds2"}
    #         }
    #         force_rerun=True,
    #         cancel_on_kill=True,
    #         location="US",
    # )

    # #trigger task

    # # check operator will expect some return cols or rows else will show error
    # merge_query_job = BigQueryCheckOperator(
    #     task_id="merge_query_job",
    #     sql=f"delete from gls-customer-poc.ds2.source staging where 1-1; \n \
    #           insert into gls-customer-poc.ds2.source staging \
    #           select from gls-customer poc.ds2.source; \
    #           select count (1) as row count from gls-customer-poc.ds2.source_staging ;",  # at end, select query require for check operator
    #     use legacy _sql=False,
    #     location="US",
    # )
    
    def start_merge_query_time (ti):
        start_tm = datetime.now().strftime('%Y-%m-%d %H-%M-%S-%E')
        ti.xcom_push (key='start_tm', value-start_tm)


    start_merge_query_time = PythonOperator (
        task id='start_merge_query_time',
        provide context=True,
        python_callable=_start_merge_query_time,
    )

    merge_query job = BigQueryOperator (
        task id='merge_query_job',
        use_legacy_sql=False,
        write disposition='WRITE TRUNCATE',
        allow large results=True,
        sql='''
        #standardSQL
        delete from `gls-customer-poc.ds2.source` staging where 1=1;
        insert into `gls-customer-poc.ds2.source_staging` select * from `gls-customer-poc.ds2.source`;
        ''',
        dag-dag

    def _end_merge_query_time (ti):
        end tm = datetime.now().strftime('%Y-%m-%d %H-SM-S-SE')
        start_tm = ti.xcom pull (key='start_tm', task_ids="start_merge_query_time")
        logger.log struct({
            "batch id": Variable.get("batch id"),
        }, severity="WARNING")
        
    end_merge_query_time = PythonOperator (
        task id='end_merge_query_time',
        provide context=True,
        python_callable=_end_merge_query_time,
    )  

    # start >> read_config_db >> branch_task  \
    # >> [DummyOperator(task_id=f"tk1_(table)", dag=dag) for table in return_config()]  \
    # >> end

    if return_config():
        for table in return_config(): 
            taskname=table + "_TaskGrp"
            with TaskGroup(taskname) as ingest_table_group:
                tk1 = DummyOperator(task_id=f"tkl_(table)", dag=dag) 
                tk2 = DummyOperator(task_id-f"tk2_(table)", dag=dag)
                
                PYSPARK_JOB = {
                    "reference": {"project_id": PROJECT_ID},
                    "placement": {"cluster_name": CLUSTER_NAME},
                    "pyspark_job": {
                        "main python_file_uri": "gs://<bucket>/notebooks/jupyter/pyjob.py", 
                        "args": [Variable.get("batch_id"), logger_name, table]
                    },
                }
                pyspark_task= DataprocSubmitJobOperator(
                    task_id=f"pyspark_task_{table}", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
                )

                branch_task >> tk1 >> tk2 >> end
                logging.info(f"{table = }")

    start >> read_config_db >> branch_task >> ingest_table_group >> end