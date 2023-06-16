# python 3.9
# https://www.python.org/ftp/python/3.9.13/python-3.9.13-amd64.exe

# Các gói cần cài đặt
# pip install SQLAlchemy
# pip install psycopg2
# pip install psycopg2-binary
# pip install pyodbc
# pip install pandas

import pyodbc
import psycopg2
import pandas as pd
import sqlite3

# SQL info
import sqlalchemy
from pandas import array
from sqlalchemy import create_engine

# schema pg name
schema_pg = 'etl_northwind'

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
pg_db = 'northwind_v3'


def extract_sql():
    try:
        # SQL Server connection details
        sql_server_conn_str = f'DRIVER={sql_driver};SERVER={sql_server};DATABASE={sql_database};UID={sql_user};PWD={sql_pwd}'
        sql_server_conn = pyodbc.connect(sql_server_conn_str)

        # SQL query to extract data
        src_cursor = sql_server_conn.cursor()

        # execute_query
        src_cursor.execute("""select t.name as table_name from sys.tables t where t.name in ('Categories', 
        'CustomerCustomerDemo', 'CustomerDemographics', 'Customers', 'Employees', 'EmployeeTerritories', 
        'OrderDetails', 'Orders', 'Products', 'Region', 'Shippers', 'Suppliers', 'Territories') """)

        src_tables = src_cursor.fetchall()
        print(src_tables)
        for tbl in src_tables:
            query = f"SELECT * FROM {tbl[0]}"
            df = pd.read_sql(query, sql_server_conn)
            df_transform = transform_table(tbl[0], df)
            load_to_pg(df_transform, tbl[0])

    except Exception as ex:
        print('data extract error: ' + str(ex))
    finally:
        sql_server_conn.close()


def transform_table(tbl, df_sql):
    try:
        # change col name
        data_transform = []
        data_transform_col = None
        data_transform_type = None
        if tbl == 'Categories':
            df_sql['Picture'] = df_sql['Picture'].apply(lambda x: bytes(x))
            data_transform_col = df_sql.rename(
                columns={"CategoryID": "category_id", "CategoryName": "category_name", "Description": "description",
                         "Picture": "picture"})
            # change dtype
            data_transform_type = {'category_id': sqlalchemy.types.Integer,
                                   'category_name': sqlalchemy.types.String,
                                   'description': sqlalchemy.types.Text,
                                   'picture': sqlalchemy.types.LargeBinary}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Region':
            data_transform_col = df_sql.rename(
                columns={"RegionID": "region_id", "RegionDescription": "region_description"}
            )
            # change dtype
            data_transform_type = {'region_id': sqlalchemy.types.Integer,
                                   'region_description': sqlalchemy.types.String}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Suppliers':
            data_transform_col = df_sql.rename(
                columns={"SupplierID": "supplier_id",
                         "CompanyName": "company_name",
                         "ContactName": "contact_name",
                         "ContactTitle": "contact_title",
                         "Address": "address",
                         "City": "city",
                         "Region": "region",
                         "PostalCode": "postal_code",
                         "Country": "country",
                         "Phone": "phone",
                         "Fax": "fax",
                         "HomePage": "home_page"
                         }
            )
            # change dtype
            data_transform_type = {
                "supplier_id": sqlalchemy.types.Integer,
                "company_name": sqlalchemy.types.String,
                "contact_name": sqlalchemy.types.String,
                "contact_title": sqlalchemy.types.String,
                "address": sqlalchemy.types.String,
                "city": sqlalchemy.types.String,
                "region": sqlalchemy.types.String,
                "postal_code": sqlalchemy.types.String,
                "country": sqlalchemy.types.String,
                "phone": sqlalchemy.types.String,
                "fax": sqlalchemy.types.String,
                "home_page": sqlalchemy.types.Text
            }
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Employees':
            df_sql['Photo'] = df_sql['Photo'].apply(lambda x: bytes(x))
            data_transform_col = df_sql.rename(
                columns={"TitleOfCourtesy": "title_of_courtesy",
                         "Title": "title",
                         "ReportsTo": "reports_to",
                         "Region": "region",
                         "PostalCode": "postal_code",
                         "PhotoPath": "photo_path",
                         "Photo": "photo",
                         "Notes": "notes",
                         "LastName": "last_name",
                         "HomePhone": "home_phone",
                         "HireDate": "hire_date",
                         "FirstName": "first_name",
                         "Extension": "extension",
                         "EmployeeID": "employee_id",
                         "Country": "country",
                         "City": "city",
                         "BirthDate": "birth_date",
                         "Address": "address"})
            # change dtype
            data_transform_type = {'title_of_courtesy': sqlalchemy.types.String,
                                   'title': sqlalchemy.types.String,
                                   'reports_to': sqlalchemy.types.SmallInteger,
                                   'region': sqlalchemy.types.String,
                                   'postal_code': sqlalchemy.types.String,
                                   'photo_path': sqlalchemy.types.String,
                                   'photo': sqlalchemy.types.LargeBinary,
                                   'notes': sqlalchemy.types.Text,
                                   'last_name': sqlalchemy.types.String,
                                   'home_phone': sqlalchemy.types.String,
                                   'hire_date': sqlalchemy.types.Date,
                                   'first_name': sqlalchemy.types.String,
                                   'extension': sqlalchemy.types.String,
                                   'employee_id': sqlalchemy.types.Integer,
                                   'country': sqlalchemy.types.String,
                                   'city': sqlalchemy.types.String,
                                   'birth_date': sqlalchemy.types.Date,
                                   'address': sqlalchemy.types.String}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'EmployeeTerritories':
            data_transform_col = df_sql.rename(
                columns={"EmployeeID": "employee_id",
                         "TerritoryID": "territory_id"
                         })
            data_transform_type = {
                "employee_id": sqlalchemy.types.Integer,
                "territory_id": sqlalchemy.types.String
            }
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Customers':
            data_transform_col = df_sql.rename(
                columns={"CustomerID": "customer_id",
                         "CompanyName": "company_name",
                         "ContactName": "contact_name",
                         "ContactTitle": "contact_title",
                         "Address": "address",
                         "City": "city",
                         "Region": "region",
                         "PostalCode": "postal_code",
                         "Country": "country",
                         "Phone": "phone",
                         "Fax": "fax",
                         })
            data_transform_type = {
                "customer_id": sqlalchemy.types.String,
                "company_name": sqlalchemy.types.String,
                "contact_name": sqlalchemy.types.String,
                "contact_title": sqlalchemy.types.String,
                "address": sqlalchemy.types.String,
                "city": sqlalchemy.types.String,
                "region": sqlalchemy.types.String,
                "postal_code": sqlalchemy.types.String,
                "country": sqlalchemy.types.String,
                "phone": sqlalchemy.types.String,
                "fax": sqlalchemy.types.String,
            }
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'CustomerCustomerDemo':
            data_transform_col = df_sql.rename(
                columns={"CustomerID": "customer_id",
                         "CustomerTypeID": "customer_type_id"
                         })
            data_transform_type = {
                "customer_id": sqlalchemy.types.String,
                "customer_type_id": sqlalchemy.types.String
            }
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'CustomerDemographics':
            data_transform_col = df_sql.rename(
                columns={"CustomerTypeID": "customer_type_id",
                         "CustomerDesc": "customer_desc"
                         })
            data_transform_type = {
                "customer_type_id": sqlalchemy.types.String,
                "customer_desc": sqlalchemy.types.Text
            }
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Products':
            data_transform_col = df_sql.rename(
                columns={"ProductID": "product_id",
                         "ProductName": "product_name",
                         "SupplierID": "supplier_id",
                         "CategoryID": "category_id",
                         "QuantityPerUnit": "quantity_per_unit",
                         "UnitPrice": "unit_price",
                         "UnitsInStock": "units_in_stock",
                         "UnitsOnOrder": "units_on_order",
                         "ReorderLevel": "reorder_level",
                         "Discontinued": "discontinued"})

            data_transform_type = {"product_id": sqlalchemy.types.Integer,
                                   "product_name": sqlalchemy.types.String,
                                   "supplier_id": sqlalchemy.types.Integer,
                                   "category_id": sqlalchemy.types.Integer,
                                   "quantity_per_unit": sqlalchemy.types.String,
                                   "unit_price": sqlalchemy.types.REAL,
                                   "units_in_stock": sqlalchemy.types.Integer,
                                   "units_on_order": sqlalchemy.types.Integer,
                                   "reorder_level": sqlalchemy.types.Integer,
                                   "discontinued": sqlalchemy.types.BOOLEAN}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Shippers':
            data_transform_col = df_sql.rename(
                columns={"ShipperID": "shipper_id",
                         "CompanyName": "company_name",
                         "Phone": "phone"
                         })

            data_transform_type = {"shipper_id": sqlalchemy.types.Integer,
                                   "company_name": sqlalchemy.types.String,
                                   "phone": sqlalchemy.types.String}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Orders':
            data_transform_col = df_sql.rename(
                columns={"OrderID": "order_id",
                         "CustomerID": "customer_id",
                         "EmployeeID": "employee_id",
                         "OrderDate": "order_date",
                         "RequiredDate": "required_date",
                         "ShippedDate": "shipped_date",
                         "ShipVia": "ship_via",
                         "Freight": "freight",
                         "ShipName": "ship_name",
                         "ShipAddress": "ship_address",
                         "ShipCity": "ship_city",
                         "ShipRegion": "ship_region",
                         "ShipPostalCode": "ship_postal_code",
                         "ShipCountry": "ship_country"
                         })

            data_transform_type = {
                "order_id": sqlalchemy.types.Integer,
                "customer_id": sqlalchemy.types.String,
                "employee_id": sqlalchemy.types.Integer,
                "order_date": sqlalchemy.types.Date,
                "required_date": sqlalchemy.types.Date,
                "shipped_date": sqlalchemy.types.Date,
                "ship_via": sqlalchemy.types.Integer,
                "freight": sqlalchemy.types.FLOAT,
                "ship_name": sqlalchemy.types.String,
                "ship_address": sqlalchemy.types.String,
                "ship_city": sqlalchemy.types.String,
                "ship_region": sqlalchemy.types.String,
                "ship_postal_code": sqlalchemy.types.String,
                "ship_country": sqlalchemy.types.String}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'OrderDetails':
            data_transform_col = df_sql.rename(
                columns={"OrderID": "order_id",
                         "ProductID": "product_id",
                         "UnitPrice": "unit_price",
                         "Quantity": "quantity",
                         "Discount": "discount"
                         })

            data_transform_type = {
                "order_id": sqlalchemy.types.Integer,
                "product_id": sqlalchemy.types.Integer,
                "unit_price": sqlalchemy.types.Float,
                "quantity": sqlalchemy.types.Integer,
                "discount": sqlalchemy.types.REAL}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        if tbl == 'Territories':
            data_transform_col = df_sql.rename(
                columns={"TerritoryID": "territory_id",
                         "TerritoryDescription": "territory_description",
                         "RegionID": "region_id"
                         })

            data_transform_type = {
                "territory_id": sqlalchemy.types.String,
                "territory_description": sqlalchemy.types.String,
                "region_id": sqlalchemy.types.Integer}
            data_transform.append(data_transform_col)
            data_transform.append(data_transform_type)
            return data_transform

        return data_transform

    except Exception as e:
        print('Transform table error:' + str(e))


def load_to_pg(df, tbl):
    try:
        rows_imported = 0
        # PostgresSQL connection details
        pg_conn = create_engine(
            url="postgresql://{0}:{1}@{2}:{3}/{4}".format(pg_user, pg_pwd, pg_server, '5432', pg_db))

        # save df to postgresql
        tbl_name_transform = None

        # ở đây dùng if else ko dùng match case để chạy đc trên các phiên bản python nhỏ hơn 3.10
        if tbl == "Categories":
            tbl_name_transform = "categories"
        elif tbl == "CustomerCustomerDemo":
            tbl_name_transform = "customer_customer_demo"
        elif tbl == "CustomerDemographics":
            tbl_name_transform = "customer_demographics"
        elif tbl == "Customers":
            tbl_name_transform = "customers"
        elif tbl == "Employees":
            tbl_name_transform = "employees"
        elif tbl == "EmployeeTerritories":
            tbl_name_transform = "employee_territories"
        elif tbl == "OrderDetails":
            tbl_name_transform = "order_details"
        elif tbl == "Orders":
            tbl_name_transform = "orders"
        elif tbl == "Products":
            tbl_name_transform = "products"
        elif tbl == "Region":
            tbl_name_transform = "region"
        elif tbl == "Shippers":
            tbl_name_transform = "shippers"
        elif tbl == "Suppliers":
            tbl_name_transform = "suppliers"
        elif tbl == "Territories":
            tbl_name_transform = "territories"

        # Load the data into PostgresSQL
        df_sql = df[0]
        df_type = df[1]

        df_sql.to_sql(tbl_name_transform, pg_conn, schema=f"{schema_pg}", if_exists='replace', index=False,
                      dtype=df_type)

        # rows_imported += len(df_sql)
        print(tbl_name_transform)
        print('Data imported successful')
    except Exception as e:
        print(tbl)
        print('Data import error:' + str(e))
    finally:
        pg_conn.dispose()


def create_schema(schema_name):
    try:
        conn = psycopg2.connect(
            host=pg_server,
            database=pg_db,
            user=pg_user,
            password=pg_pwd
        )

        cur = conn.cursor()

        # Define the SQL statement to create a schema
        create_schema_query = f"CREATE SCHEMA {schema_name};"

        # Execute the query to create the schema
        cur.execute(create_schema_query)

        # Commit the transaction
        conn.commit()

        # Close the cursor and the connection
        cur.close()
        conn.close()
    except Exception as ex:
        print('Create schema: ' + str(ex))


def load_data():
    try:
        print('vao load data')
        conn = psycopg2.connect(
            host=pg_server,
            database=pg_db,
            user=pg_user,
            password=pg_pwd
        )
        cur = conn.cursor()

        # Query the table names from the metadata
        cur.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_pg}'
        """)

        # Fetch all table names from the result set
        table_names = [row[0] for row in cur.fetchall()]

        # Iterate over the table names and transfer data to the "public" schema
        for table_name in table_names:
            engine = create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(pg_user, pg_pwd, pg_server, '5432', pg_db))

            df_schema = pd.read_sql_table(table_name, engine, schema=schema_pg)

            target_schema = "public"
            df_public = pd.read_sql_table(table_name, engine, schema=target_schema)

            load_table(df_schema, df_public, table_name, engine)

        # Close connection
        cur.close()
        conn.close()
    except Exception as ex:
        print('Error load_data : ' + str(ex))


def load_table(source_df, target_df, table_name, engine):
    try:
        unique_identifier = ''
        if table_name == 'categories':
            unique_identifier = 'category_id'
            dtype = {'foreign_key_column': sqlalchemy.ForeignKey('referenced_table.referenced_column')}
        elif table_name == 'customer_customer_demo':
            unique_identifier = 'customer_type_id'
        elif table_name == 'customers':
            unique_identifier = 'customer_id'
        elif table_name == 'employee_territories':
            unique_identifier = 'territory_id'
        elif table_name == 'employees':
            unique_identifier = 'employee_id'
        elif table_name == 'order_details':
            unique_identifier = 'customer_id'
        elif table_name == 'orders':
            unique_identifier = 'order_id'
        elif table_name == 'products':
            unique_identifier = 'product_id'
        elif table_name == 'region':
            unique_identifier = 'region_id'
        elif table_name == 'shippers':
            unique_identifier = 'shipper_id'
        elif table_name == 'suppliers':
            unique_identifier = 'supplier_id'
        elif table_name == 'territories':
            unique_identifier = 'territory_id'
        elif table_name == 'us_states':
            unique_identifier = 'state_id'

        # Update the target table with the new values from the source dataframe
        source_df.to_sql(table_name, engine, if_exists='replace', index=False)

    except Exception as ex:
        print('Error load_table : ' + str(ex))



# đây là các bước chạy của quá trình ETL dữ liệu
# b1: Đầu tiên sẽ tạo một 1 schema trên data của Postgresql với tên schema đc nhập vào từ biến schema_pg
# b2: Tiếp theo tiến hành extrac và transform dữ liệu từ SQL server sang schema vừa tạo ở Postgresql
# b3: Bước cuối cùng update and create data từ schema postgres vào db chính của postgres

try:
    # create schema pg (b1)
    # create_schema(schema_pg)
    # extract and transform (b2)
    # extract_sql()
    # load db pg (b3)
    load_data()
except Exception as ex:
    print("Error Run function init: " + str(ex))


