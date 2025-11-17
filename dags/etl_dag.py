from datetime import datetime, timedelta

import os

os.getenv("MY_API_KEY")


from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.transformed_data import run_transform          # must exist in etl/transform.py
from etl.loading_postgres import load_to_postgres  # must exist in etl/loading_postgres.py


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_neon_dag",
    default_args=default_args,
    description="ETL pipeline using Neon as the DB",
    schedule_interval="@daily",   # or None for manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "neon"],
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl_transform",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    etl_task >> load_task
