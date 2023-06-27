from airflow.decorators import dag, task
from datetime import timedelta, datetime
import psycopg2

import sys

sys.path.append("create_dim_fact_tb_v2.py")
sys.path.append("update_or_create_data_v2.py")

import create_dim_fact_tb, update_or_create_data

default_args = {
    'owner': 'alphateam',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id='load_datamart_v2.6',
     default_args=default_args,
     start_date=datetime.now(),
     schedule_interval='@daily')
def run_etl():
    # print('v2')
    @task()
    def create_schema():
        create_dim_fact_tb.create_schema_data()

    @task()
    def create_tb_dim_products():
        create_dim_fact_tb.create_tb_dim_products()

    @task()
    def create_tb_dim_revenue_per_cus():
        create_dim_fact_tb.create_tb_dim_revenue_per_cus()

    @task()
    def create_tb_dim_metric():
        create_dim_fact_tb.create_tb_dim_metric()

    @task()
    def create_tb_dim_region_w_customer():
        create_dim_fact_tb.create_tb_dim_region_w_customer()

    @task()
    def create_tb_fact_orders():
        create_dim_fact_tb.create_tb_fact_orders()

    @task()
    def create_tb_dim_date():
        create_dim_fact_tb.create_tb_dim_date()

    @task()
    def create_tb_dim_region():
        create_dim_fact_tb.create_tb_dim_region()

    @task()
    def load_data():
        # print('s2')
        update_or_create_data.init_update_or_create()

    create_schema() >> [create_tb_dim_products(), create_tb_dim_revenue_per_cus(), create_tb_dim_metric(), create_tb_dim_region_w_customer(), create_tb_fact_orders(), create_tb_dim_date(), create_tb_dim_region()] >> load_data()


run_etl()
