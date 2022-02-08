import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
import datetime as dt
import uuid
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator

dag = DAG(
	dag_id="umbrella",
	start_date=dt.datetime(2022, 2, 3),
	end_date=dt.datetime(year=2022, month=2, day=10),
	schedule_interval=dt.timedelta(days=3),
	catchup=False,
)

start = DummyOperator(task_id="start")

fetch_weather_data = BashOperator(task_id="fetch_weather_data",bash_command="echo fetch_weather_data",dag=dag,)
clean_weather_data = BashOperator(task_id="clean_weather_data",bash_command="echo clean_weather_data",dag=dag,)

fetch_sales_data = BashOperator(task_id="fetch_sales_data",bash_command="echo fetch_sales_data",dag=dag,)
clean_sales_data = BashOperator(task_id="clean_sales_data",bash_command="echo clean_sales_data",dag=dag,)

join_data_sets = BashOperator(task_id="join_data_sets",bash_command="echo join_data_sets",dag=dag,)

def _train_model(**context):
	model_id = str(uuid.uuid4())
	context["task_instance"].xcom_push(key="model_id", value=model_id)

train_model = PythonOperator(task_id="train_model",python_callable=_train_model,dag=dag,)


def _deploy_model(**context):
		model_id = context["task_instance"].xcom_pull(task_ids="train_model", key="model_id")
		print(f"Deploying model {model_id}")
deploy_model = PythonOperator(task_id="deploy_model",python_callable=_deploy_model,)

latest_only = LatestOnlyOperator(
	task_id="latest_only",
	dag=dag,
)

start >> [fetch_weather_data, fetch_sales_data]

fetch_weather_data >> clean_weather_data
fetch_sales_data >> clean_sales_data

[clean_weather_data, clean_sales_data] >> join_data_sets

join_data_sets >> train_model >> latest_only >> deploy_model
