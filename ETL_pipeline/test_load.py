import pandas as pd
import pyodbc
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine

# Connect to SQL Server and retrieve data
sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=192.168.100.27;DATABASE=northwind_sql;UID=root;PWD=admin123')
query = "SELECT * FROM Employees"
df_sql = pd.read_sql(query, sql_conn)

# Transform 'image' column to bytes
df_sql['Photo'] = df_sql['Photo'].apply(lambda x: bytes(x))

# df_sql = df_sql.drop('EmployeeID', axis=1)
df_sql = df_sql.rename(
    columns={"TitleOfCourtesy": "title_of_courtesy", "Title": "title", "ReportsTo": "reports_to", "Region": "region",
             "PostalCode": "postal_code",
             "PhotoPath": "photo_path", "Photo": "photo", "Notes": "notes", "LastName": "last_name",
             "HomePhone": "home_phone", "HireDate": "hire_date", "FirstName": "first_name", "Extension": "extension",
             "EmployeeID": "employee_id", "Country": "country", "City": "city", "BirthDate": "birth_date",
             "Address": "address"})


# Connect to PostgreSQL
pg_conn = psycopg2.connect(host='103.130.215.192', database='northwind_v1', user='postgres', password='admin12345')
engine = create_engine('postgresql+psycopg2://postgres:admin12345@103.130.215.192/northwind_v1')

# Load data into PostgreSQL
df_sql.to_sql('etl_employees', engine, if_exists='replace', index=False, dtype={'title_of_courtesy': sqlalchemy.types.String,
                             'title':  sqlalchemy.types.String,
                             'reports_to': sqlalchemy.types.SmallInteger,
                             'region': sqlalchemy.types.String,
                             'postal_code': sqlalchemy.types.String,
                            'photo_path': sqlalchemy.types.String,
                            'photo': sqlalchemy.types.LargeBinary,
                            'notes': sqlalchemy.types.Text,
                            'last_name': sqlalchemy.types.String,'home_phone': sqlalchemy.types.String,'hire_date': sqlalchemy.types.Date,'first_name': sqlalchemy.types.String,'extension': sqlalchemy.types.String,'employee_id': sqlalchemy.types.Integer,'country': sqlalchemy.types.String,'city': sqlalchemy.types.String,'birth_date': sqlalchemy.types.Date,'address': sqlalchemy.types.String})

# Close connections
sql_conn.close()
pg_conn.close()
