# from airflow.decorators import dag, task
# from datetime import timedelta, datetime
#
# default_args = {
#     'owner': 'tri',
#     'retries': 5,
#     'retry_delay': timedelta(minutes=5)
# }
#
# @dag(dag_id='tri_test_01',
#      default_args=default_args,
#      start_date=datetime.now(),
#      schedule_interval='@daily')
# def hi_etl():
#
#     @task()
#     def task_1():
#         return 'task 1'
#
#     @task()
#     def task_2():
#         return 'task 2'
#
#     @task()
#     def run_task(t1, t2):
#         print(f'funtion {t1} and funtion {t2}')
#
#     t1 = task_1()
#     t2 = task_2()
#     run_task(t1, t2)
#
#
# hi_etl()
#
#
# # from datetime import datetime, timedelta
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # import psycopg2
# #
# # import sys
# # sys.path.append("create_dim_fact_tb.py")
# # sys.path.append("update_or_create_data.py")
# #
# # import create_dim_fact_tb, update_or_create_data
# #
# #
# # default_args = {
# #     'owner' : 'alphateam',
# #     'retries': 5,
# #     'retry_delay' : timedelta(minutes=2)
# # }
# #
# # def create_schema():
# #     # print('tmp1')
# #     create_dim_fact_tb.run_task()
# #
# #
# # def load_data():
# #     # print('tmp2')
# #     update_or_create_data.init_update_or_create()
# #
# #
# # with DAG(
# #     default_args=default_args,
# #     dag_id='run_flows',
# #     description='tri test write code',
# #     # start_date=datetime(2023,6,25),
# #     start_date=datetime.now(),
# #     # schedule_interval='@daily'
# #     schedule_interval=None
# # ) as dag:
# #     task1 = PythonOperator(
# #         task_id='create_schema',
# #         python_callable=create_schema
# #     )
# #     task2 = PythonOperator(
# #         task_id='load_data',
# #         python_callable=load_data
# #     )
# #     task1 >> task2