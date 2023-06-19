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
        contact_title character varying(30),
        address character varying(60),
        city character varying(15),
        region character varying(15),
        postal_code character varying(10),
        country character varying(15),
        phone character varying(24),
        fax character varying(24)
    );
"""
cur.execute(create_tb_dim_customer)

# Create a table dim_products
table_name = "dim_products"
create_tb_dim_products = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        product_id smallint NOT NULL PRIMARY KEY,
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

# Create a table fact
table_name = "fact_orders"
create_tb_dim_date = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        order_id int NOT NULL PRIMARY KEY,
        customer_id bpchar,
        product_id smallint,
        order_date date,
        required_date date,
        shipped_date date,
        unit_price real NOT NULL,
        quantity smallint NOT NULL,
        discount real NOT NULL,
        freight real,
        Cumulated_Percentage int,
        Cumulated_Sales int,
        Customer_Sales int,
        Customer_Sales_Group int,
        Cumulated_Percentage_Region int,
        Cumulated_Sales_Region int,
        Region_Group int,
        Region_Sales int,
        FOREIGN KEY (customer_id) REFERENCES datamart_customer.dim_customer (customer_id),
        FOREIGN KEY (product_id) REFERENCES datamart_customer.dim_products (product_id)
    );
"""
cur.execute(create_tb_dim_date)

# Create a table dim_date
table_name = "dim_date"
create_tb_dim_date = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id int NOT NULL PRIMARY KEY,
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

# Close the cursor and connection
cur.close()
conn.close()
