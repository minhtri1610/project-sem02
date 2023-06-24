from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

import sys
sys.path.append("create_dim_fact_tb.py")
sys.path.append("update_or_create_data.py")

import create_dim_fact_tb, update_or_create_data


default_args = {
    'owner' : 'alphateam',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}

def create_schema():
    create_dim_fact_tb.run_task()


def load_data():
    update_or_create_data.init_update_or_create()


with DAG(
    default_args=default_args,
    dag_id='test_alpha_team',
    description='tri test write code',
    start_date=datetime(2023,6,27),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema
    )
    task2 = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    task1 >> task2