import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag_1 = DAG(
	dag_id="dag_1",
	start_date=airflow.utils.dates.days_ago(3),
	schedule_interval="0 16 * * *",
	catchup=False,
)

task_1 = BashOperator(task_id="task_1",bash_command="echo {{ dag_run.get_task_instance('task_1').start_date }}",dag=dag_1,)
task_2 = BashOperator(task_id="task_2",bash_command="echo task_2",dag=dag_1,)

trigger = TriggerDagRunOperator(
	task_id="trigger",
	trigger_dag_id="dag_2",
	dag=dag_1,
)

task_1 >> task_2 >> trigger

dag_2 = DAG(
	dag_id="dag_2",
	start_date=airflow.utils.dates.days_ago(3),
	schedule_interval=None,
)

task_3 = BashOperator(task_id="task_3",bash_command="echo task_3",dag=dag_2,)
task_4 = BashOperator(task_id="task_4",bash_command="echo task_3",dag=dag_2,)

task_3 >> task_4