from datetime import datetime

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy import DummyOperator

import json
from airflow.models.connection import Connection



dag = DAG(
    'mysql',
    start_date=datetime(2021, 1, 1),
    default_args={'mysql_conn_id': 'mysql_conn'},
    tags=['example'],
    catchup=False,
)

start = DummyOperator(task_id="start")

# [START howto_operator_mysql]

mysql_task  = MySqlOperator(
    task_id='create_table_mysql', sql=r"""select * from roles;""", dag=dag
)



start >> mysql_task