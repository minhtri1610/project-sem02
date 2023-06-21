import psycopg2

pg_user = 'postgres'
pg_pwd = 'admin12345'
pg_server = '103.130.215.192'
pg_db = 'northwind_v4'

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host=pg_server,
    database=pg_db,
    user=pg_user,
    password=pg_pwd
)
# Create a cursor object to execute SQL queries
cur = conn.cursor()

# delete schema
# DROP SCHEMA IF EXISTS datamart_customer_old CASCADE;
print('Bắt đầu tiến trình tạo datamart')

# Create a schema
schema_name = "datamart_customer"
create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
cur.execute(create_schema_query)

# Create a table dim_customer
table_name = "dim_customer"
create_tb_dim_customer = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        customer_id bpchar NOT NULL PRIMARY KEY,
        company_name character varying(40) NOT NULL,
        contact_name character varying(30),
        address character varying(60),
        city character varying(15),
        region character varying(15),
        postal_code character varying(10),
        country character varying(15)
    );
"""
cur.execute(create_tb_dim_customer)

# Create a table dim_products
table_name = "dim_products"
create_tb_dim_products = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        product_id int NOT NULL PRIMARY KEY,
        product_name character varying(40) NOT NULL,
        quantity_per_unit character varying(20),
        unit_price real,
        units_in_stock smallint,
        units_on_order smallint,
        reorder_level smallint,
        discontinued integer NOT NULL
    );
"""
cur.execute(create_tb_dim_products)

# Create a table dim_employees
table_name = "dim_employees"
create_tb_dim_employees = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        employee_id int NOT NULL PRIMARY KEY,
        last_name character varying(20),
        first_name character varying(10),
        title character varying(30),
        city character varying(15),
        region character varying(15),
        country character varying(15),
        reports_to smallint
    );
"""
cur.execute(create_tb_dim_employees)

# Create a table fact
table_name = "fact_orders"
create_tb_fact_orders = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        order_id int NOT NULL PRIMARY KEY,
        customer_id bpchar,
        product_id int,
        employee_id int,
        required_date date,
        shipped_date date,
        unit_price real NOT NULL,
        quantity smallint NOT NULL,
        discount real NOT NULL,
        freight real,
        cumulated_percentage int,
        cumulated_sales int,
        customer_sales int,
        customer_sales_group int,
        cumulated_percentage_region int,
        cumulated_sales_region int,
        region_group int,
        region_sales int,
        FOREIGN KEY (customer_id) REFERENCES datamart_customer.dim_customer (customer_id),
        FOREIGN KEY (product_id) REFERENCES datamart_customer.dim_products (product_id),
        FOREIGN KEY (employee_id) REFERENCES datamart_customer.dim_employees (employee_id)
    );
"""
cur.execute(create_tb_fact_orders)

# Create a table dim_date
table_name = "dim_date"
create_tb_dim_date = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL NOT NULL PRIMARY KEY,
        order_id int NOT NULL,
        order_date date,
        year INT,
        month INT,
        day INT,
        short_name text,
        quarter INT,
        FOREIGN KEY (order_id) REFERENCES datamart_customer.fact_orders (order_id)
    );
"""
cur.execute(create_tb_dim_date)

# Commit the changes to the database
conn.commit()

print('Hoàn tất tiến trình tạo datamart')

# Close the cursor and connection
cur.close()
conn.close()
