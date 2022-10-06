from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
import json

default_dag_args = {
    'email_on_failure': True,
    'email': ['olena.back@gmail.com'],
    'retries': 2,
}
with DAG('process_sales_flow',
         start_date=datetime(2022, 8, 9),
         end_date=datetime(2022, 8, 12),
         schedule_interval='0 1 * * *',
         catchup=True,
         max_active_runs=1,
         default_args=default_dag_args,
         ) as dag:
    fetch_task = SimpleHttpOperator(
        task_id='extract_data_from_api',
        method='POST',
        headers={"Content-Type": "application/json"},
        http_conn_id='sales_api',
        data=json.dumps({"date": "{{ds}}", "raw_dir": "raw/sales/{{ds}}"}),
    )

    convert_task = SimpleHttpOperator(
        task_id='convert_to_avro',
        method='POST',
        headers={"Content-Type": "application/json"},
        http_conn_id='convert_api',
        data=json.dumps({"stg_dir": "stg/sales/{{ds}}", "raw_dir": "raw/sales/{{ds}}"}),
    )

fetch_task >> convert_task
