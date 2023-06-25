from airflow.decorators import dag, task
from datetime import timedelta, datetime
import psycopg2

import sys

sys.path.append("create_dim_fact_tb.py")
sys.path.append("update_or_create_data.py")

import create_dim_fact_tb, update_or_create_data

default_args = {
    'owner': 'alphateam',
    'retries': 50,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='load_datamart_v1',
     default_args=default_args,
     start_date=datetime.now(),
     schedule_interval='@daily')
def run_etl():

    @task()
    def create_schema():
        # print('s1')
        create_dim_fact_tb.run_task()

    @task()
    def load_data():
        # print('s2')
        update_or_create_data.init_update_or_create()

    create_schema() >> load_data()


run_etl()
