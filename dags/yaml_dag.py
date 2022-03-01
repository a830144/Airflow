from airflow.operators.bash import BashOperator
from airflow import DAG
import datetime as dt
import yaml

dag = DAG(
	dag_id="yaml_print",
	start_date=dt.datetime(2022, 2, 3),
	end_date=dt.datetime(year=2022, month=3, day=15),
	schedule_interval=dt.timedelta(days=3),
	catchup=False,
)

with open("/root/airflow/dags/config.yaml") as config_file:
	config = yaml.load(config_file)


yaml_print = BashOperator(task_id="yaml_print_task",bash_command="input=config['input_path'] ; echo $input",dag=dag,)

