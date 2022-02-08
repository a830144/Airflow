import datetime as dt
from pathlib import Path
from airflow import DAG
from airflow.sensors.python import PythonSensor
dag = DAG(
	dag_id="supermarket",
	start_date=dt.datetime(2022, 2, 3),
	end_date=dt.datetime(year=2022, month=2, day=10),
	schedule_interval=dt.timedelta(days=3),
	concurrency=50,
	catchup=False,
)


def _wait_for_supermarket(supermarket_id):
	supermarket_path = Path("/data/" + supermarket_id)
	data_files = supermarket_path.glob("data-*.csv")
	success_file = supermarket_path / "_SUCCESS"
	return data_files and success_file.exists()
wait_for_supermarket_1 = PythonSensor(
	task_id="wait_for_supermarket_1",
	python_callable=_wait_for_supermarket,
	op_kwargs={"supermarket_id": "supermarket1"},
	dag=dag,
)