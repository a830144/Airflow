import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
dag = DAG(
	dag_id="chapter4_stocksense_bashoperator",
	start_date=airflow.utils.dates.days_ago(3),
	schedule_interval="@hourly",
)
get_data = BashOperator(
	task_id="get_data",
	bash_command=(
		"curl -o /tmp/wikipageviews.gz "
		"https://dumps.wikimedia.org/other/pageviews/"
		"{{ execution_date.year }}/"
		"{{ execution_date.year }}-"
		"{{ '{:02}'.format(execution_date.month) }}/"
		"pageviews-{{ execution_date.year }}"
		"{{ '{:02}'.format(execution_date.month) }}"
		"{{ '{:02}'.format(execution_date.day) }}-"
		"{{ '{:02}'.format(execution_date.hour) }}0000.gz"
	),
	dag=dag,
)


extract_gz = BashOperator(
	task_id="extract_gz",
	bash_command="gunzip --force /tmp/wikipageviews.gz",
	dag=dag,
)

def _fetch_pageviews(pagenames):
	result = dict.fromkeys(pagenames, 0)
	with open(f"/tmp/wikipageviews", "r") as f:
		for line in f:
			domain_code, page_title, view_counts, _ = line.split(" ")
			if domain_code == "en" and page_title in pagenames:
				result[page_title] = view_counts
	print(result)
	# Prints e.g. "{'Facebook': '778', 'Apple': '20', 'Google': '451','Amazon': '9', 'Microsoft': '119'}"

fetch_pageviews = PythonOperator(
	task_id="fetch_pageviews",
	python_callable=_fetch_pageviews,
	op_kwargs={
		"pagenames": {
			"Google",
			"Amazon",
			"Apple",
			"Microsoft",
			"Facebook",
		}
	},
	dag=dag,
)