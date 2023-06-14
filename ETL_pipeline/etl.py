# python 3.9
#https://www.python.org/ftp/python/3.9.13/python-3.9.13-amd64.exe

# Các gói cần cài đặt
# pip install SQLAlchemy
# pip install psycopg2
# pip install psycopg2-binary
# pip install pyodbc
# pip install pandas

import pyodbc
import psycopg2
import pandas as pd

# SQL info
sql_pwd = 'admin123'
sql_user = 'root'
# sql db info
sql_driver = '{SQL Server}'
sql_server = '192.168.100.27'
sql_database = 'northwind_sql'

# PG info
pg_user = 'postgres'
pg_pwd = 'admin12345'
pg_server = '103.130.215.192'
pg_db = 'northwind_v1'

# pg_user = 'tg_retrieval'
# pg_pwd = 'noatake'
# pg_server = '153.126.149.63'
# pg_db = 'test_northwind'


def extract_sql():
    try:
        # SQL Server connection details
        sql_server_conn_str = f'DRIVER={sql_driver};SERVER={sql_server};DATABASE={sql_database};UID={sql_user};PWD={sql_pwd}'
        sql_server_conn = pyodbc.connect(sql_server_conn_str)

        # SQL query to extract data
        src_cursor = sql_server_conn.cursor()
        # execute_query
        src_cursor.execute(""" select t.name as table_name 
                from sys.tables t where t.name in ('Customers') """)

        src_tables = src_cursor.fetchall()

        for tbl in src_tables:
            print(tbl[0])
            df = pd.read_sql_query(f'select * from {tbl[0]}', sql_server_conn)
            load_to_pg(df, tbl[0])

    except Exception as ex:
        print('data extract error: ' + str(ex))
    finally:
        sql_server_conn.close()


def load_to_pg(df, tbl):
    try:
        rows_imported = 0
        # PostgreSQL connection details
        pg_conn = psycopg2.connect(
            host=f"{pg_server}",
            port="5432",
            database=f"{pg_db}",
            user=f"{pg_user}",
            password=f"{pg_pwd}"
        )
        # save df to postgresql
        print(tbl)
        tbl_name_transform = None
        if tbl == "Customers":
            tbl_name_transform = "customers"
        # elif tbl == "Products":
        #     tbl_name_transform = "products"
        # elif tbl == "Orders":
        #     tbl_name_transform = "orders"
        # elif tbl == "OrderDetails":
        #     tbl_name_transform = "order_details"

        # Load the data into PostgreSQL
        print(pg_conn)
        df.to_sql(f"{tbl_name_transform}", pg_conn, if_exists='replace', index=False)

        rows_imported += len(df)

        print('Data imported successful')
    except Exception as e:
        print('Data import error:' + str(e))
    finally:
        pg_conn.close()


try:
    extract_sql()
except Exception as ex:
    print("Error " + str(ex))
