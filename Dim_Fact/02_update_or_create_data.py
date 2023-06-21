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
        #lấy dữ liệu từ bảng nguồn
        if target_table == 'fact_orders':
            #các cột chưa lấy đc
            # freight
            # cumulated_percentage
            # cumulated_sales
            # customer_sales
            # customer_sales_group
            # cumulated_percentage_region
            # cumulated_sales_region
            # region_group
            # region_sales

            dbcursor.execute(f"""
                SELECT od.order_id,
                    od.customer_id,
                    od.employee_id,
                    odt.product_id,
                    odt.unit_price,
                    odt.quantity,
                    odt.discount,
                    od.required_date,
                    od.shipped_date FROM {source_schema}.{source_table} as od
                LEFT JOIN {source_schema}.order_details as odt ON odt.order_id = od.order_id
            """)
        elif target_table == 'dim_date':
            #chưa có cột short_name
            dbcursor.execute(f"""
                                SELECT fod.order_id,
                                    od.order_date,
                                    date_part('year',od.order_date) as year,
                                    date_part('month',od.order_date) as month,
                                    date_part('day',od.order_date) as day,
                                    '' as short_name,
                                    date_part('quarter',od.order_date) as quarter
                                FROM {target_schema}.{source_table} as fod
                                INNER JOIN {source_schema}.orders as od on od.order_id = fod.order_id
                            """)
        else:
            dbcursor.execute(f'SELECT {columns_source} FROM {source_schema}.{source_table}')

        #lấy toàn bộ dữ liệu
        data_rows = dbcursor.fetchall()

        #duyệt qua tất cả các record của data nguồn để xem nên insert hay update
        for row in data_rows:
            # Insert or update data in the data mart
            # Check if the record exists in the data mart

            dbcursor.execute(f'SELECT * FROM {target_schema}.{target_table} WHERE {col_id} = %s', (row[0],))
            existing_record = dbcursor.fetchone()
            print(row)
            if existing_record:
                print('update')
                # # Update the existing record
                execute_to_table(dbcursor, target_table, row, 'update')
            else:
                print('insert')
                # Insert a new record
                execute_to_table(dbcursor, target_table, row, 'insert')

        # Commit the changes and close connections
        conn.commit()
        dbcursor.close()

    except Exception as e:
        print('Error:' + str(e))


def execute_to_table(dbcursor, target_table, row, type_action):
    try:

        if target_table == 'dim_customer':
            #khóa chính
            col_id = 'customer_id'
            #định vị thứ tự cột trong mảng
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

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET last_name = %s, first_name = %s, title = %s, city = %s, region = %s, country = %s, reports_to = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['last_name']], row[index['first_name']], row[index['title']], row[index['city']], row[index['region']], row[index['country']], row[index['reports_to']], row[index['employee_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (employee_id, last_name, first_name, title, city, region, country, reports_to) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['employee_id']], row[index['last_name']], row[index['first_name']], row[index['title']], row[index['city']], row[index['region']], row[index['country']], row[index['reports_to']], ))


        if target_table == 'dim_products':
            col_id = 'product_id'
            index = {'product_id': 0, 'product_name': 1, 'quantity_per_unit': 2, 'unit_price': 3, 'units_in_stock': 4, 'units_on_order': 5, 'reorder_level': 6, 'discontinued': 7}

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET product_name = %s, quantity_per_unit = %s, unit_price = %s, units_in_stock = %s, units_on_order = %s, reorder_level = %s, discontinued = %s WHERE {col_id} = %s'
                dbcursor.execute(update_query, (row[index['product_name']], row[index['quantity_per_unit']], row[index['unit_price']], row[index['units_in_stock']], row[index['units_on_order']], row[index['reorder_level']], row[index['discontinued']], row[index['product_id']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (product_id, product_name, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['product_id']], row[index['product_name']], row[index['quantity_per_unit']], row[index['unit_price']], row[index['units_in_stock']], row[index['units_on_order']], row[index['reorder_level']], row[index['discontinued']], ))

        if target_table == 'fact_orders':
            col_id = 'order_id'
            index = {'order_id': 0, 'customer_id': 1, 'employee_id': 2, 'product_id': 3, 'unit_price': 4,
                     'quantity': 5, 'discount': 6, 'required_date': 7, 'shipped_date': 8}

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET customer_id = %s, employee_id = %s, product_id = %s, unit_price = %s, quantity = %s, discount = %s, required_date = %s, shipped_date = %s WHERE {col_id} = %s'
                # print(insert_query)
                dbcursor.execute(update_query, (
                    row[index['customer_id']], row[index['employee_id']],
                    row[index['product_id']], row[index['unit_price']], row[index['quantity']],
                    row[index['discount']], row[index['required_date']], row[index['shipped_date']], row[index['order_id']]))
            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (order_id, customer_id, employee_id, product_id, unit_price, quantity, discount, required_date, shipped_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                dbcursor.execute(insert_query, (
                row[index['order_id']], row[index['customer_id']], row[index['employee_id']],
                row[index['product_id']], row[index['unit_price']], row[index['quantity']],
                row[index['discount']], row[index['required_date']], row[index['shipped_date']]))

        if target_table == 'dim_date':
            col_id = 'order_id'
            index = {'order_id': 0, 'order_date': 1, 'year': 2, 'month': 3, 'day': 4,
                     'short_name': 5, 'quarter': 6}

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET order_id = %s, order_date = %s, year = %s, month = %s, day = %s, short_name = %s, quarter = %s  WHERE {col_id} = %s'
                # print(insert_query)
                dbcursor.execute(update_query, (
                    row[index['order_id']], row[index['order_date']],
                    row[index['year']], row[index['month']], row[index['day']],
                    row[index['short_name']], row[index['quarter']],row[index['order_id']]))
            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (order_id, order_date, year, month, day, short_name, quarter) VALUES (%s, %s, %s, %s, %s, %s, %s)'
                dbcursor.execute(insert_query, (
                    row[index['order_id']], row[index['order_date']], row[index['year']],
                    row[index['month']], row[index['day']], row[index['short_name']],
                    row[index['quarter']]))

    except Exception as e:
        print('Error execute_to_table:' + str(e))


def import_data(table_name):
    try:
        if table_name == 'dim_customer':
            # columns
            columns_source = ('customer_id, company_name, contact_name, address, city, region, postal_code, country')
            columns_target = []
            update_or_create_db(columns_source, 'customers', columns_target, table_name, 'customer_id')
            print('insert or update table dim_customer successfully')

        if table_name == 'dim_employees':
            # columns
            columns_source = ('employee_id, last_name, first_name, title, city, region, country, reports_to')
            columns_target = []
            update_or_create_db(columns_source, 'employees', columns_target, table_name, 'employee_id')
            print('insert or update table dim_employees successfully')

        if table_name == 'dim_products':
            # columns
            columns_source = ('product_id, product_name, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued')
            columns_target = []
            update_or_create_db(columns_source, 'products', columns_target, table_name, 'product_id')
            print('insert or update table dim_products successfully')

        if table_name == 'fact_orders':
            # các cột có trong bảng orders và order_detail
            # order_id
            # customer_id
            # product_id
            # employee_id
            # required_date
            # shipped_date
            # unit_price
            # quantity
            # discount

            # những cột tính toán
            # freight
            # cumulated_percentage
            # cumulated_sales
            # customer_sales
            # customer_sales_group
            # cumulated_percentage_region
            # cumulated_sales_region
            # region_group
            # region_sales

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, 'orders', columns_target, 'fact_orders', 'order_id')
            print('insert or update table fact_orders successfully')

        if table_name == 'dim_date':
            # id
            # order_id
            # order_date
            # year
            # month
            # day
            # short_name
            # quarter

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, 'fact_orders', columns_target, 'dim_date', 'order_id')
            # print('insert or update table dim_products successfully')
    except Exception as e:
        print('Error import_data:' + str(e))


try:
    print('Tiến trình đang chạy...')

    print('-- Bắt đầu tải bảng dim_customer... --')
    import_data('dim_customer')
    print('-- Hoàn tất tải bảng dim_customer... --')

    print('-- Bắt đầu tải bảng dim_employees... --')
    import_data('dim_employees')
    print('-- Hoàn tất tải bảng dim_employees... --')

    print('-- Bắt đầu tải bảng dim_products... --')
    import_data('dim_products')
    print('-- Hoàn tất tải bảng dim_products... --')

    print('-- Bắt đầu tải bảng fact_orders... --')
    import_data('fact_orders')
    print('-- Hoàn tất tải bảng fact_orders... --')

    print('-- Bắt đầu tải bảng dim_date... --')
    import_data('dim_date')
    print('-- Hoàn tất tải bảng dim_date... --')

    print('Hoàn tất tiến trình.')
    conn.close()
except Exception as e:
    print('Error:' + str(e))
