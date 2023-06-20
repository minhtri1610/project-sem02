import psycopg2

# PG info
pg_user = 'postgres'
pg_pwd = 'admin12345'
pg_server = '103.130.215.192'
pg_db = 'northwind_v4'

conn = psycopg2.connect(
    host=pg_server,
    database=pg_db,
    user=pg_user,
    password=pg_pwd
)

# Define the source schema and table
source_schema = 'public'
# source_table = 'customers'

# Define the target schema and table
target_schema = 'datamart_customer'


def update_or_create_db(columns_source, source_table, columns_target, target_table, col_id):
    try:
        dbcursor = conn.cursor()
        dbcursor.execute(f'SELECT {columns_source} FROM {source_schema}.{source_table}')
        data_rows = dbcursor.fetchall()

        for row in data_rows:
            # Insert or update data in the data mart
            # Check if the record exists in the data mart

            dbcursor.execute(f'SELECT * FROM {target_schema}.{target_table} WHERE {col_id} = %s', (row[0],))
            existing_record = dbcursor.fetchone()

            if existing_record:
                # # Update the existing record
                execute_to_table(dbcursor, target_table, row, 'update')
            else:
                # Insert a new record
                execute_to_table(dbcursor, target_table, row, 'insert')

        # Commit the changes and close connections
        conn.commit()
        dbcursor.close()
        conn.close()

    except Exception as e:
        print('Error:' + str(e))


def execute_to_table(dbcursor, target_table, row, type_action):
    try:

        if target_table == 'dim_customer':
            col_id = 'customer_id'
            index = {'customer_id': 0, 'company_name': 1, 'contact_name': 2, 'address': 3, 'city': 4, 'region': 5, 'postal_code': 6, 'country': 7}

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET company_name = %s, contact_name = %s, address = %s, city = %s, region = %s, postal_code = %s, country = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['company_name']], row[index['contact_name']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], row[index['customer_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (customer_id, company_name, contact_name, address, city, region, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['customer_id']], row[index['company_name']], row[index['contact_name']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], ))

        if target_table == 'dim_employees':
            col_id = 'employee_id'
            index = {'employee_id': 0, 'last_name': 1, 'first_name': 2, 'title': 3, 'city': 4, 'region': 5, 'country': 6, 'reports_to': 7}

            # if type_action == 'update':
            #     update_query = f'UPDATE {target_schema}.{target_table} SET company_name = %s, contact_name = %s, address = %s, city = %s, region = %s, postal_code = %s, country = %s WHERE {col_id} = %s'
            #     dbcursor.execute(update_query, (row[index['company_name']], row[index['contact_name']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], row[index['customer_id']]))
            #
            # elif type_action == 'insert':
            #     insert_query = f'INSERT INTO {target_schema}.{target_table} (customer_id, company_name, contact_name, address, city, region, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
            #     # print(insert_query)
            #     dbcursor.execute(insert_query, (row[index['customer_id']], row[index['company_name']], row[index['contact_name']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], ))


        if target_table == 'dim_products':
            col_id = 'product_id'









            index = {'product_id': 0, 'product_name': 1, 'quantity_per_unit': 2, 'unit_price': 3, 'units_in_stock': 4, 'units_on_order': 5, 'reorder_level': 6, 'discontinued': 7}

            # if type_action == 'update':
            #     update_query = f'UPDATE {target_schema}.{target_table} SET company_name = %s, contact_name = %s, address = %s, city = %s, region = %s, postal_code = %s, country = %s WHERE {col_id} = %s'
            #     dbcursor.execute(update_query, (row[index['company_name']], row[index['contact_name']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], row[index['customer_id']]))
            #
            # elif type_action == 'insert':
            #     insert_query = f'INSERT INTO {target_schema}.{target_table} (customer_id, company_name, contact_name, address, city, region, postal_code, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
            #     # print(insert_query)
            #     dbcursor.execute(insert_query, (row[index['customer_id']], row[index['company_name']], row[index['contact_name']], row[index['address']], row[index['city']], row[index['region']], row[index['postal_code']], row[index['country']], ))

    except Exception as e:
        print('Error execute_to_table:' + str(e))


def import_data(table_name):
    try:
        if table_name == 'dim_customer':
            # columns
            columns_source = ('customer_id, company_name, contact_name, address, city, region, postal_code, country')
            columns_target = []
            update_or_create_db(columns_source, 'customers', columns_target, 'dim_customer', 'customer_id')
            print('insert or update table dim_customer successfully')

        if table_name == 'dim_employees':
            # columns
            columns_source = ('employee_id, last_name, first_name, title, city, region, country, reports_to')
            columns_target = []
            update_or_create_db(columns_source, 'employees', columns_target, 'dim_employees', 'employee_id')
            print('insert or update table dim_employees successfully')

        if table_name == 'dim_products':
            # columns
            columns_source = ('product_id, product_name, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued')
            columns_target = []
            update_or_create_db(columns_source, 'products', columns_target, 'dim_products', 'product_id')
            print('insert or update table dim_products successfully')

        # if table_name == 'fact_orders':
            # order_id
            # customer_id
            # product_id
            # employee_id
            # required_date
            # shipped_date
            # unit_price
            # quantity
            # discount
            # freight
            # cumulated_percentage
            # cumulated_sales
            # customer_sales
            # customer_sales_group
            # cumulated_percentage_region
            # cumulated_sales_region
            # region_group
            # region_sales

            # columns_source = (
            #     'product_id, product_name, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued')
            # columns_target = []
            # update_or_create_db(columns_source, 'products', columns_target, 'dim_products', 'product_id')
            # print('insert or update table dim_products successfully')

        # if table_name == 'dim_date':
            # id
            # order_id
            # order_date
            # year
            # month
            # day
            # short_name
            # quarter

            # columns_source = (
            #     'order_id, product_name, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued')
            # columns_target = []
            # update_or_create_db(columns_source, 'products', columns_target, 'dim_products', 'product_id')
            # print('insert or update table dim_products successfully')


    except Exception as e:
        print('Error import_data:' + str(e))


try:
    import_data('dim_customer')
except Exception as e:
    print('Error:' + str(e))
