from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from datetime import datetime, timedelta
from <dags>.<folder>.read_cloudsql_postgres import set_configfile

default_args = {
    "start_date': datetime (2022, 3, 1), #example date
    "depends_on_past" : False,
    "retries": 1,
    "retry_delay": timedelta (minutes=3),
}

with DAG(
	dag_id="dag_configfile_set", 
    schedule interval=None,
    tags=['ingestion'],
    default_args=default_args, 
    catchup=False,) as dag:
	
	start=DummyOperator(task_id="start", dag=dag) 
    end=DummyOperator(task_id="end", dag=dag)
	
	wait_dag = BashOperator(
		task_id="wait_dag",
		bash_command = "sleep 5s"
	)
	
	config_prop_file_set = PythonOperator(
		task_id="config_prop_file_set",
		python_callable=set_configfile,
		#requirements = [""],
		#system_site_packages=True,
		dag=dag
	)
	
	trigger_dynamic_task_group_dag = TriggerDagRunOperator(
		task_id="trigger_dynamic_task_group_dag",
		trigger_dag_id="dag_dynamic_task",
		execution_date="{{ ts }}",  # use ts instead of ds if you are triggering multiple times in a same day.
		wait_for_completion=False,
		dag=dag
	)
	
	
	start >> config_prop_file_set >> wait_dag  \
	>> trigger_dynamic_task_group_dag >> end
	