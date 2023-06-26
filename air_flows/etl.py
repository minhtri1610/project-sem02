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
sql_server = 'INTELPC'
sql_database = 'northwind_sql3'

# PG info
pg_user = 'postgres'
pg_pwd = 'admin12345'
pg_server = '103.130.215.192'
pg_db = 'northwind_etl'


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
        'Order Details', 'Orders', 'Products', 'Region', 'Shippers', 'Suppliers', 'Territories') """)

        src_tables = src_cursor.fetchall()

        for tbl in src_tables:
            print('---')
            print(tbl[0])
            print('+++')
            query = f"SELECT * FROM [{tbl[0]}]"
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
                "customer_id": sqlalchemy.types.String(255),
                "company_name": sqlalchemy.types.String(255),
                "contact_name": sqlalchemy.types.String(255),
                "contact_title": sqlalchemy.types.String(255),
                "address": sqlalchemy.types.String(255),
                "city": sqlalchemy.types.String(255),
                "region": sqlalchemy.types.String(255),
                "postal_code": sqlalchemy.types.String(255),
                "country": sqlalchemy.types.String(255),
                "phone": sqlalchemy.types.String(255),
                "fax": sqlalchemy.types.String(255),
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
                                   "company_name": sqlalchemy.types.String(255),
                                   "phone": sqlalchemy.types.String(255)}
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

        if tbl == 'OrderDetails' or tbl == 'Order Details':
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
        elif tbl == "Order Details":
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
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"

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

            load_table(table_name,schema_pg, conn)

        # Close connection
        cur.close()
        conn.close()
    except Exception as ex:
        print('Error load_data : ' + str(ex))


def load_table(table_name, schema_source, conn):
    try:
        schema_targe = 'public'
        unique_identifier = ''
        dbcursor = conn.cursor()

        if table_name == 'categories':
            print('tbl categories')
            col_id = 'category_id'
            dbcursor.execute(f"""
                            SELECT ct.category_id, ct.category_name, ct.description, ct.picture
                            FROM {schema_source}.{table_name} as ct
                        """)
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s', (row[0],))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')

        elif table_name == 'customer_customer_demo':
            print('tbl customer_customer_demo')
            col_id = 'customer_id'
            dbcursor.execute(f"""SELECT ct.customer_id, ct.customer_type_id
                                        FROM {schema_source}.{table_name} as ct
                                    """)
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s', (row[0],))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'customer_demographics':
            print('tbl customer_demographics')
            col_id = 'customer_type_id'
            dbcursor.execute(f"""SELECT cd.customer_type_id, cd.customer_desc
                                                    FROM {schema_source}.{table_name} as cd
                                                """)
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s', (row[0],))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'customers':
            print('tbl customers')
            col_id = 'customer_id'
            dbcursor.execute(f"""SELECT c.customer_id, c.company_name, c.company_name, c.contact_name, c.contact_title, c.address, c.city, c.region, c.postal_code, c.country, c.phone, c.fax
                                                               FROM {schema_source}.{table_name} as c
                                                           """)
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s', (row[0],))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'employee_territories':
            print('tbl employee_territories')
            col_id = 'employee_id'
            unique_identifier = 'territory_id'
            dbcursor.execute(f"""SELECT et.employee_id, et.territory_id FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s and {unique_identifier} = %s', (row[0],row[1]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')

        elif table_name == 'employees':
            print('tbl employees')
            col_id = 'employee_id'
            dbcursor.execute(f"""SELECT et.employee_id, et.last_name, et.first_name, et.title, et.title_of_courtesy, et.birth_date, et.hire_date, et.address, et.city , et.region, et.postal_code, et.country, et.home_phone, et.extension, et.photo, et.notes, et.reports_to, et.photo_path FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s',(row[0]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'order_details':
            print('tbl order_details')
            unique_identifier = 'product_id'
            col_id = 'order_id'
            dbcursor.execute(
                f"""SELECT et.order_id, et.product_id, et.unit_price, et.quantity, et.discount FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s and {unique_identifier} = %s', (row[0], row[1]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'orders':
            print('tbl orders')
            col_id = 'order_id'
            dbcursor.execute(
                f"""SELECT et.order_id, et.customer_id, et.employee_id, et.order_date, et.required_date,
                et.shipped_date, et.ship_via, et.freight, et.ship_name, et.ship_address, et.ship_city,
                et.ship_region, et.ship_postal_code, et.ship_country FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s',
                    (row[0]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'products':
            print('tbl products')
            col_id = 'product_id'
            dbcursor.execute(
                f"""SELECT  et.product_id, et.product_name, et.supplier_id, et.category_id, et.quantity_per_unit,
                et.unit_price, et.units_in_stock, et.units_on_order, et.reorder_level, et.discontinued
                FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s',
                    (row[0]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'region':
            print('tbl region')
            col_id = 'region_id'
            dbcursor.execute(
                f"""SELECT  et.region_id, et.region_description
                            FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s',
                    (row[0]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'shippers':
            print('tbl shippers')
            col_id = 'shipper_id'
            dbcursor.execute(
                f"""SELECT  et.shipper_id, et.company_name , et.phone 
                                        FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s',
                    (row[0]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'suppliers':
            print('tbl suppliers')
            col_id = 'supplier_id'
            dbcursor.execute(
                f"""SELECT et.supplier_id, et.company_name, et.contact_name, et.contact_title, et.address, et.city, et.region, et.postal_code, et.country, et.phone, et.fax, et.home_page FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s',
                    (row[0]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'territories':
            print('tbl territories')
            col_id = 'territory_id'
            dbcursor.execute(
                f"""SELECT et.territory_id, et.territory_description, et.region_id FROM {schema_source}.{table_name} as et""")
            data_rows = dbcursor.fetchall()
            # duyệt qua tất cả các record của data nguồn để xem nên insert hay update
            for row in data_rows:
                dbcursor.execute(
                    f'SELECT * FROM {schema_targe}.{table_name} WHERE {col_id} = %s',
                    (row[0]))
                existing_record = dbcursor.fetchone()
                print(row)
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, table_name, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, table_name, row, 'insert')
        elif table_name == 'us_states':
            print('tbl us_states')
            unique_identifier = 'state_id'

    except Exception as ex:
        print('Error load_table : ' + str(ex))


def execute_to_table(dbcursor, table_name, row, type_action):
    try:
        schema_target = 'public'
        if table_name == 'categories':
            col_id = 'category_id'
            #định vị thứ tự cột trong mảng
            index = {'category_id': 0, 'category_name': 1, 'description': 2, 'picture': 3}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET category_name = %s, description = %s, picture = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['category_name']], row[index['description']], row[index['picture']], row[index['category_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (category_id, category_name, description, picture) VALUES (%s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['category_id']], row[index['category_name']], row[index['description']], row[index['picture']]))

        if table_name == 'customer_customer_demo':
            col_id = 'customer_id'
            #định vị thứ tự cột trong mảng
            index = {'customer_id': 0, 'customer_type_id': 1}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET customer_type_id = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['customer_type_id']], row[index['customer_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (customer_id, customer_type_id) VALUES (%s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['customer_id']], row[index['customer_type_id']]))

        if table_name == 'customer_demographics':
            col_id = 'customer_type_id'
            #định vị thứ tự cột trong mảng
            index = {'customer_type_id': 0, 'customer_desc': 1}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET customer_desc = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['customer_desc']], row[index['customer_type_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (customer_type_id, customer_desc) VALUES (%s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['customer_type_id']], row[index['customer_desc']]))

        if table_name == 'customers':
            col_id = 'customer_id'
            #định vị thứ tự cột trong mảng
            index = {'customer_id': 0, 'company_name': 1, 'contact_name': 2, 'contact_title': 3, 'address': 4, 'city': 5, 'region': 6, 'postal_code':7, 'phone': 8, 'fax': 9}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET company_name = %s, contact_name = %s, contact_title = %s, address = %s, city = %s, region = %s, postal_code = %s, phone = %s, fax = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['company_name']], row[index['contact_name']], row[index['contact_title']], row[index['address']], row[index['city']], str(row[index['region']]), str(row[index['postal_code']]), str(row[index['phone']]), str(row[index['fax']]), str(row[index['customer_id']])))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, phone, fax) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, str((row[index['customer_id']]), row[index['company_name']], row[index['contact_name']], row[index['contact_title']], row[index['address']], row[index['city']], str(row[index['region']]), str(row[index['postal_code']]), str(row[index['phone']]), str(row[index['fax']])))

        if table_name == 'employee_territories':
            col_id = 'employee_id'
            #định vị thứ tự cột trong mảng
            index = {'employee_id': 0, 'territory_id': 1}

            if type_action == 'update':
                print('update')
                # update_query = f'UPDATE {schema_target}.{table_name} SET employee_id = %s, description = %s, picture = %s WHERE {col_id} = %s'
                # dbcursor.execute(update_query, (row[index['category_name']], row[index['description']], row[index['picture']], row[index['category_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (employee_id, territory_id) VALUES (%s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['employee_id']], row[index['territory_id']]))

        if table_name == 'employees':
            col_id = 'employee_id'
            #định vị thứ tự cột trong mảng
            index = {'employee_id': 0, 'last_name': 1, 'first_name': 2, 'title': 3, 'title_of_courtesy': 4, 'birth_date': 5, 'hire_date': 6, 'address': 7, 'city': 8, 'region': 9, 'postal_code': 10, 'country': 11, 'home_phone': 12, 'extension': 13, 'photo': 14, 'notes': 15, 'reports_to': 16, 'photo_path': 17}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET last_name = %s, first_name = %s, title = %s, title_of_courtesy = %s, birth_date = %s, hire_date = %s, address = %s, city = %s, region = %s, postal_code = %s, country = %s, home_phone = %s, extension = %s, photo = %s, notes = %s, reports_to = %s, photo_path = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['last_name']], row[index['first_name']], row[index['title']], row[index['title_of_courtesy']], row[index['birth_date']], row[index['hire_date']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], row[index['home_phone']], row[index['extension']], row[index['photo']], row[index['notes']], row[index['reports_to']], row[index['photo_path']], row[index['employee_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['employee_id']], row[index['last_name']], row[index['first_name']], row[index['title']], row[index['title_of_courtesy']], row[index['birth_date']], row[index['hire_date']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], row[index['home_phone']], row[index['extension']], row[index['photo']], row[index['notes']], row[index['reports_to']], row[index['photo_path']]))

        if table_name == 'order_details':
            col_id = 'order_id'

            #định vị thứ tự cột trong mảng
            index = {'order_id': 0, 'product_id': 1, 'unit_price': 2, 'quantity': 3, 'discount': 4}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET unit_price = %s, quantity = %s, discount = %s WHERE {col_id} = %s and product_id = %s'
                dbcursor.execute(update_query, (row[index['unit_price']], row[index['quantity']], row[index['discount']], row[index['order_id']], row[index['product_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (order_id, product_id, unit_price, quantity, discount) VALUES (%s, %s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['order_id']], row[index['product_id']], row[index['unit_price']], row[index['quantity']], row[index['discount']]))

        if table_name == 'orders':
            col_id = 'order_id'
            #định vị thứ tự cột trong mảng
            index = {'order_id': 0,'customer_id': 1,'employee_id': 2,'order_date': 3,'required_date': 4,'shipped_date': 5,'ship_via': 6,'freight': 7,'ship_name': 8,'ship_address': 9,'ship_city': 10, 'ship_region': 11,'ship_postal_code': 12,'ship_country': 13}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET customer_id = %s, employee_id = %s, ' \
                               f'order_date = %s, required_date = %s, shipped_date = %s, ship_via = %s, freight = %s, ' \
                               f'ship_name = %s, ship_address = %s, ship_city = %s, ship_region = %s, ' \
                               f'ship_postal_code = %s, ship_country = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['customer_id']],row[index['employee_id']],row[index['order_date']],row[index['required_date']],row[index['shipped_date']],row[index['ship_via']],row[index['freight']],row[index['ship_name']],row[index['ship_address']],row[index['ship_city']],row[index['ship_region']],row[index['ship_postal_code']],row[index['ship_country']],row[index['order_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (order_id,customer_id,employee_id,' \
                               f'order_date,required_date,shipped_date,ship_via,freight,ship_name,ship_address,' \
                               f'ship_city,ship_region,ship_postal_code,ship_country) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['order_id']],row[index['customer_id']],row[index['employee_id']],row[index['order_date']],row[index['required_date']],row[index['shipped_date']],row[index['ship_via']],row[index['freight']],row[index['ship_name']],row[index['ship_address']],row[index['ship_city']],row[index['ship_region']],row[index['ship_postal_code']],row[index['ship_country']]))

        if table_name == 'products':
            col_id = 'product_id'
            #định vị thứ tự cột trong mảng
            index = { 'product_id': 0, 'product_name': 1, 'supplier_id': 2, 'category_id': 3, 'quantity_per_unit': 4, 'unit_price': 5, 'units_in_stock': 6, 'units_on_order': 7, 'reorder_level': 8, 'discontinued': 9}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET product_name = %s, supplier_id = %s, ' \
                               f'category_id = %s, quantity_per_unit = %s, unit_price = %s, units_in_stock = %s, ' \
                               f'units_on_order = %s, reorder_level = %s, discontinued = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['product_name']], row[index['supplier_id']], row[index['category_id']], row[index['quantity_per_unit']], row[index['unit_price']], row[index['units_in_stock']], row[index['units_on_order']], row[index['reorder_level']], row[index['discontinued']],row[index['product_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} ( product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['product_id']], row[index['product_name']], row[index['supplier_id']], row[index['category_id']], row[index['quantity_per_unit']], row[index['unit_price']], row[index['units_in_stock']], row[index['units_on_order']], row[index['reorder_level']], row[index['discontinued']]))

        if table_name == 'region':
            col_id = 'region_id'
            #định vị thứ tự cột trong mảng
            index = {'region_id': 0, 'region_description': 1}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET region_description = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['region_description']], row[index['region_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (region_id, region_description) VALUES (%s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['region_id']], row[index['region_description']]))

        if table_name == 'shippers':
            col_id = 'shipper_id'
            #định vị thứ tự cột trong mảng
            index = {'shipper_id': 0, 'company_name': 1, 'phone': 2}
            print(row[index['shipper_id']])
            print(row[index['company_name']])
            print(row[index['phone']])
            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET company_name = %s, phone = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['company_name']], row[index['phone']], row[index['shipper_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (shipper_id, company_name, phone) VALUES (%s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['shipper_id']], row[index['company_name']], row[index['phone']]))

        if table_name == 'suppliers':
            col_id = 'supplier_id'
            #định vị thứ tự cột trong mảng
            index = { 'supplier_id': 0, 'company_name': 1, 'contact_name': 2, 'contact_title': 3, 'address': 4, 'city': 5, 'region': 6, 'postal_code': 7, 'country': 8, 'phone': 9, 'fax': 10, 'homepage': 11}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET company_name = %s, contact_name = %s, contact_title = %s, address = %s, city = %s, region = %s, postal_code = %s, country = %s, phone = %s, fax = %s, homepage = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['company_name']], row[index['contact_name']], row[index['contact_title']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], row[index['phone']], row[index['fax']], row[index['homepage']], row[index['supplier_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (supplier_id,company_name,contact_name,' \
                               f'contact_title,address,city,region,postal_code,country,phone,fax,homepage) VALUES (' \
                               f'%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['supplier_id']], row[index['company_name']], row[index['contact_name']], row[index['contact_title']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], row[index['phone']], row[index['fax']], row[index['homepage']]))

        if table_name == 'territories':
            col_id = 'territory_id'
            #định vị thứ tự cột trong mảng
            index = {'territory_id': 0, 'territory_description': 1, 'region_id': 2}

            if type_action == 'update':
                update_query = f'UPDATE {schema_target}.{table_name} SET territory_description = %s, region_id = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['territory_description']], row[index['region_id']], row[index['territory_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {schema_target}.{table_name} (territory_id, territory_description, region_id) VALUES (%s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['territory_id']], row[index['territory_description']], row[index['region_id']]))

    except Exception as e:
        print('Error execute_to_table:' + str(e))


# đây là các bước chạy của quá trình ETL dữ liệu
# b1: Đầu tiên sẽ tạo một 1 schema trên data của Postgresql với tên schema đc nhập vào từ biến schema_pg
# b2: Tiếp theo tiến hành extrac và transform dữ liệu từ SQL server sang schema vừa tạo ở Postgresql
# b3: Bước cuối cùng update and create data từ schema postgres vào db chính của postgres

try:
    print('Bắt đầu tạo schema tạm')
    # create schema pg (b1)
    create_schema(schema_pg)
    print('Hoàn tất tạo schema')

    print('Bắt đầu tiến trình chuyển data từ SQL vào schema tạm Postgres')
    # extract and transform (b2)
    extract_sql()
    print('Hoàn Tất tiến trình bước 2')

    print('Bắt đầu tiến trình load_data b3')
    # load db pg (b3)
    load_data()
    print('Hoàn tất tiến trình b3')

except Exception as ex:
    print("Error Run function init: " + str(ex))


