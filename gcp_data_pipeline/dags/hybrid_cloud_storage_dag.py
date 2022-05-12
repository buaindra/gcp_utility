import datetime
from airflow import models


default_dag_args = {
    "start_date": datetime.datetime.today(),
}


with models.DAG(
    "sample_dag"
    ,schedule_interval=datetime.timedelta(days=1)
    ,default_args= default_dag_args) as dag:

    
