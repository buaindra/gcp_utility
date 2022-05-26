from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging


def get_config(SRC_TBL_NM="ALL"):
    where_additional = ""
    if SRC_TBL_NM.upper() != "ALL":
        where_additional = f" AND 'SRC_TBL_NM' = '{SRC_TBL_NM}'"
    logging.info(f"{where_additional = }")
    lst = range(0,2,1)
    return lst

with DAG(
    dag_id="test_sample_dag",
    default_args={
        "owner": "indranil",
        "start_date": datetime.now(),
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": None
    },
    schedule_interval = None,
    catchup = False,
    params={"SRC_TBL_NM": "test_table"}
) as dag:

    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)

    