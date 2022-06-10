'''
gsutil cp/working/dynamic_task_group_dag.py gs://us-centrall test-dcd1a744-bucket/dags/
gsutil cp/working/ConfigFile.properties gs://us-central1-test-dcd1a744-bucket/data/
'''

import string
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow import models
from datetime import datetime, timedelta
from airflow.models.param import Param  #added this to use Param object
from airflow.models import Variable  #added this to use Variable
import logging  #added this to use Logging INFO
import configparser

#import os
#import numpy as np  #for np. fromiter

project_id=models.Variable.get("project_id"))
region=models.Variable.get("region")

env "DEV
config=confignarser.ConfigParser()
config.read(/home/airflow/ges/data/ConfigFile.properties")
default_tables=config.get(env, "tables_name").split(", ")[:1]
Logging.info(f"{default_tables}")

# hard coded table

# default_tables = []
# for i in range (1,11):
#     table_name = "table_" + str(i) 
#     default_tables.append(table_name)

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
    SRC_TBL_NM = kwargs["dag_run"].conf.get("SRC_TBL_NM", [])
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
    dag_id="TEST_PL_Indra", 
    schedule interval=None,
    tags=['ingestion'],
    default_args=default_args, 
    catchup=False,
    params={ 
        'SRC_TBL_NM': Param("ALL", type=['string', 'array']), # default ALL and use json schema validata
    },
) as dag:

    #Dummy DAGS
    start=DummyOperator(task_id="start", dag=dag) 
    end=DummyOperator(task_id="end", dag-dag, trigger_rule="none_failed_or_ski

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

    # start >> read_config_db >> branch_task  \
    # >> [DummyOperator(task_id=f"tk1_(table)", dag=dag) for table in return_config()]  \
    # >> end

    if return_config():
        for table in return_config(): 
            taskname=table + "_TaskGrp"
            with TaskGroup(taskname) as ingest_table_group:
                tk1 = DummyOperator(task_id=f"tkl_(table)", dag=dag) 
                tk2 = DummyOperator(task_id-f"tk2_(table)", dag=dag)

                branch_task >> tk1 >> tk2 >> end
                logging.info(f"{table = }")

    start >> read_config_db >> branch_task >> ingest_table_group >> end