import psycopg2
from datetime import timedelta, datetime

# PG info
pg_user = 'postgres'
pg_pwd = 'admin123'
pg_server = 'localhost'
pg_db = 'northwind_etl'

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
            dbcursor.execute(f"""
                SELECT od.order_id,
                    od.customer_id,
                    odt.product_id,
                    odt.unit_price,
                    odt.quantity,
                    odt.discount,
                    od.freight,
                    od.order_date,
                    od.required_date,
                    od.shipped_date FROM {source_schema}.{source_table} as od
                LEFT JOIN {source_schema}.order_details as odt ON odt.order_id = od.order_id
            """)
        elif target_table == 'dim_revenue_per_cus':
            dbcursor.execute(f""" WITH customer_revenue AS ( SELECT c.customer_id, SUM(od.unit_price * od.quantity) 
            AS revenue, AVG(od.unit_price * od.quantity) AS average_order_value, date_part('year', o.order_date) as year, date_part('month', o.order_date) as month,date_part('quarter', o.order_date) as quarter FROM {source_schema}.customers c 
                                    JOIN {source_schema}.orders o ON c.customer_id = o.customer_id
                                    JOIN {source_schema}.order_details od ON o.order_id = od.order_id
                                    GROUP BY c.customer_id, year, month, quarter
                                ),
                                quartiles AS (
                                    SELECT
                                        (SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY revenue) FROM customer_revenue) AS Q1,
                                        (SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY revenue) FROM customer_revenue) AS Q3
                                )
                                SELECT
                                    customer_id,
                                    revenue,
                                    CASE
                                        WHEN revenue >= (SELECT Q3 FROM quartiles) THEN 'High-Value Buyer'
                                        ELSE 'Low-Value Buyer'
                                    END AS high_low_byers,
                                    average_order_value,
                                    year,
                                    month,
                                    quarter
                                FROM
                                    customer_revenue ORDER BY revenue DESC
                                """)
        elif target_table == 'dim_region_w_customer':
            dbcursor.execute(f"""
                SELECT C.region,
                    COUNT ( C.customer_id ) AS count_customer
                FROM
                    {source_schema}.customers C
                GROUP BY
                    C.region;
            """)
        elif target_table == 'dim_metric':
            print('dim_metric')
            dbcursor.execute(f"""
                select drpc.year, SUM(drpc.revenue) as total_sale, (tbl_top_25.total_revenue_top / SUM(drpc.revenue)) * 100 as contribution_precent
                from {target_schema}.dim_revenue_per_cus as drpc
                LEFT JOIN (
                    SELECT SUM(total) AS total_revenue_top , year
                    FROM (
                        select SUM(revenue) as total, "year"
                        from {target_schema}.dim_revenue_per_cus group by year, customer_id ORDER BY total DESC LIMIT 25
                        ) subquery
                    GROUP BY year
                    ORDER BY year
                ) as tbl_top_25 ON tbl_top_25.year = drpc.year
                group by drpc.year, tbl_top_25.total_revenue_top ORDER BY drpc.year;
            """)
        elif target_table == 'dim_date':
            #chưa có cột short_name
            dbcursor.execute(f"""SELECT min(fod.order_date) as min_od_date, max(fod.order_date) as max_od_date, '' as short_name
                                FROM {target_schema}.fact_orders as fod limit 1""")
        elif target_table == 'dim_region':
            dbcursor.execute(f"""
                SELECT city, country FROM {source_schema}.customers GROUP BY city, country ORDER BY country ASC
            """)
        else:
            dbcursor.execute(f'SELECT {columns_source} FROM {source_schema}.{source_table}')

        #lấy toàn bộ dữ liệu
        data_rows = dbcursor.fetchall()

        #duyệt qua tất cả các record của data nguồn để xem nên insert hay update
        for row in data_rows:
            # Insert or update data in the data mart
            # Check if the record exists in the data mart
            if target_table == 'dim_region_w_customer':
                dbcursor.execute(f'SELECT * FROM {target_schema}.{target_table} WHERE region = %s', (row[0],))
            elif target_table == 'dim_revenue_per_cus':
                dbcursor.execute(f'SELECT * FROM {target_schema}.{target_table} WHERE customer_id = %s  and year = %s', (row[0], row[3]))
            elif target_table == 'dim_date':
                # Delete all records from the table
                dbcursor.execute(f"DELETE FROM {target_schema}.{target_table}")
                # Reset the ID column
                dbcursor.execute(f"ALTER SEQUENCE {target_schema}.{target_table}_id_seq RESTART WITH 1")
            elif target_table == 'dim_metric':
                print('vao dim_mectric for')
                dbcursor.execute(f'SELECT * FROM {target_schema}.{target_table} WHERE year = %s', (row[0],))
            else:
                dbcursor.execute(f'SELECT * FROM {target_schema}.{target_table} WHERE {col_id} = %s', (row[0],))


            print(row)
            if target_table != 'dim_date':
                existing_record = dbcursor.fetchone()
                if existing_record:
                    print('update')
                    # # Update the existing record
                    execute_to_table(dbcursor, target_table, row, 'update')
                else:
                    print('insert')
                    # Insert a new record
                    execute_to_table(dbcursor, target_table, row, 'insert')
            else:
                execute_to_table_d_date(dbcursor, target_table, row)

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

        # if target_table == 'dim_employees':
        #     col_id = 'employee_id'
        #     index = {'employee_id': 0, 'last_name': 1, 'first_name': 2, 'title': 3, 'city': 4, 'region': 5, 'country': 6, 'reports_to': 7}
        #
        #     if type_action == 'update':
        #         update_query = f'UPDATE {target_schema}.{target_table} SET last_name = %s, first_name = %s, title = %s, city = %s, region = %s, country = %s, reports_to = %s WHERE {col_id} = %s'
        #         dbcursor.execute(update_query, (row[index['last_name']], row[index['first_name']], row[index['title']], row[index['city']], row[index['region']], row[index['country']], row[index['reports_to']], row[index['employee_id']]))
        #
        #     elif type_action == 'insert':
        #         insert_query = f'INSERT INTO {target_schema}.{target_table} (employee_id, last_name, first_name, title, city, region, country, reports_to) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        #         # print(insert_query)
        #         dbcursor.execute(insert_query, (row[index['employee_id']], row[index['last_name']], row[index['first_name']], row[index['title']], row[index['city']], row[index['region']], row[index['country']], row[index['reports_to']], ))

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


        if target_table == 'dim_region_w_customer':
            col_id = 'id'
            index = { 'region': 0, 'count_customer': 1}

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET region = %s, count_customer = %s WHERE region = %s'
                dbcursor.execute(update_query, (row[index['region']], row[index['count_customer']], row[index['region']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (region, count_customer) VALUES ( %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['region']], row[index['count_customer']]))


        if target_table == 'dim_revenue_per_cus':
            col_id = 'customer_id'
            index = {'customer_id': 0, 'revenue': 1, 'high_low_byers': 2, 'average_order_value': 3, 'year': 4, 'month': 5, 'quarter': 6}
            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET revenue = %s, high_low_byers = %s, average_order_value = %s, year = %s , month = %s, quarter = %s WHERE {col_id} = %s and year = %s'
                dbcursor.execute(update_query, (row[index['revenue']], row[index['high_low_byers']], row[index['average_order_value']], row[index['year']], row[index['month']], row[index['quarter']], row[index['customer_id']], row[index['year']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (customer_id, revenue, average_order_value, year, high_low_byers, month, quarter) VALUES (%s, %s, %s, %s, %s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['customer_id']], row[index['revenue']], row[index['average_order_value']], row[index['year']], row[index['high_low_byers']], row[index['month']], row[index['quarter']] ))


        if target_table == 'dim_metric':
            col_id = 'id'
            index = { 'year': 0, 'total_sale': 1, 'contribution_precent': 2}

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET total_sale = %s, contribution_precent = %s WHERE year = %s'
                dbcursor.execute(update_query, (row[index['total_sale']], row[index['contribution_precent']], row[index['year']]))

            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} ( year, total_sale, contribution_precent) VALUES (%s, %s, %s)'
                # print(insert_query)
                dbcursor.execute(insert_query, (row[index['year']], row[index['total_sale']], row[index['contribution_precent']]))

        if target_table == 'fact_orders':
            col_id = 'order_id'
            index = {'order_id': 0, 'customer_id': 1, 'product_id': 2, 'unit_price': 3,
                     'quantity': 4, 'discount': 5, 'freight': 6 , 'order_date' : 7, 'required_date': 8, 'shipped_date': 9}

            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET customer_id = %s, product_id = %s, unit_price = %s, quantity = %s, discount = %s, freight = %s, order_date = %s, required_date = %s, shipped_date = %s WHERE {col_id} = %s'
                # print(insert_query)
                dbcursor.execute(update_query, (
                    row[index['customer_id']], row[index['product_id']],
                    row[index['unit_price']], row[index['quantity']], row[index['discount']],
                    row[index['freight']], row[index['order_date']], row[index['required_date']], row[index['shipped_date']], row[index['order_id']]))
            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (order_id, customer_id, product_id, unit_price, quantity, discount, freight, order_date, required_date, shipped_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                dbcursor.execute(insert_query, (
                row[index['order_id']], row[index['customer_id']], row[index['product_id']],
                row[index['unit_price']], row[index['quantity']], row[index['discount']],
                row[index['freight']], row[index['order_date']], row[index['required_date']], row[index['shipped_date']]))

        # if target_table == 'dim_date':
        #     col_id = 'id'
        #     index = {'min_od_date': 0, 'max_od_date': 2, 'short_name': 2}

            # if type_action == 'update':
            #     update_query = f'UPDATE {target_schema}.{target_table} SET order_date = %s, year = %s, month = %s, day = %s, short_name = %s, quarter = %s  WHERE order_date = %s'
            #     # print(insert_query)
            #     dbcursor.execute(update_query, get_record_dim_date(row, type_action))
            # elif type_action == 'insert':
            #     insert_query = f'INSERT INTO {target_schema}.{target_table} (order_date, year, month, day, short_name, quarter) VALUES (%s, %s, %s, %s, %s, %s)'
            #     dbcursor.execute(insert_query, get_record_dim_date(row, type_action))

        if target_table == 'dim_region':
            col_id = 'city'
            index = {'city': 0, 'country': 1}
            if type_action == 'update':
                update_query = f'UPDATE {target_schema}.{target_table} SET city = %s, country = %s  WHERE {col_id} = %s'
                # print(insert_query)
                dbcursor.execute(update_query, (row[index['city']], row[index['country']], row[index['city']]))
            elif type_action == 'insert':
                insert_query = f'INSERT INTO {target_schema}.{target_table} (city, country) VALUES (%s, %s)'
                dbcursor.execute(insert_query, (row[index['city']], row[index['country']]))

    except Exception as e:
        print('Error execute_to_table:' + str(e))


def execute_to_table_d_date(dbcursor, target_table, row):
    range_date = get_record_dim_date(row)
    for od_date in range_date:
        date_obj = datetime.strptime(od_date, "%Y-%m-%d")
        # Extract year, month, and day
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        # Calculate the quarter
        quarter = (date_obj.month - 1) // 3 + 1

        insert_query = f'INSERT INTO {target_schema}.{target_table} (order_date, year, month, day, quarter, short_name) VALUES (%s, %s, %s, %s, %s, %s)'
        print(insert_query)
        dbcursor.execute(insert_query, (od_date, year, month, day, quarter, ''))


def get_record_dim_date(row):
    min_date = None
    max_date = None
    if row[0] != None:
        min_date = row[0]

    if row[1] != None:
        max_date = row[1]

    return create_date_range(min_date, max_date)



def create_date_range(start_date, end_date):
    # Chuyển đổi ngày bắt đầu và ngày kết thúc từ chuỗi thành đối tượng datetime
    # start_date = datetime.strptime(start_date, "%Y-%m-%d")
    # end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Tạo một danh sách để lưu trữ các ngày trong khoảng từ start_date đến end_date
    date_range = []

    # Thêm ngày bắt đầu vào danh sách
    date_range.append(start_date.strftime("%Y-%m-%d"))

    # Vòng lặp để tăng dần ngày cho đến khi đạt đến ngày kết thúc
    current_date = start_date
    while current_date < end_date:
        current_date += timedelta(days=1)  # Tăng ngày lên 1
        date_range.append(current_date.strftime("%Y-%m-%d"))

    return date_range



def import_data(table_name):
    try:
        if table_name == 'dim_customer':
            # columns
            columns_source = ('customer_id, company_name, contact_name, address, city, region, postal_code, country')
            columns_target = []
            update_or_create_db(columns_source, 'customers', columns_target, table_name, 'customer_id')
            print('insert or update table dim_customer successfully')

        # if table_name == 'dim_employees':
        #     # columns
        #     columns_source = ('employee_id, last_name, first_name, title, city, region, country, reports_to')
        #     columns_target = []
        #     update_or_create_db(columns_source, 'employees', columns_target, table_name, 'employee_id')
        #     print('insert or update table dim_employees successfully')

        if table_name == 'dim_products':
            # columns
            columns_source = ('product_id, product_name, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued')
            columns_target = []
            update_or_create_db(columns_source, 'products', columns_target, table_name, 'product_id')
            print('insert or update table dim_products successfully')

        if table_name == 'fact_orders':
            # các cột có trong bảng orders và order_detail

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, 'orders', columns_target, 'fact_orders', 'order_id')
            print('insert or update table fact_orders successfully')

        if table_name == 'dim_region_w_customer':
            # các cột có trong bảng orders và order_detail

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, '', columns_target, 'dim_region_w_customer', 'id')
            print('insert or update table dim_region_w_customer successfully')

        if table_name == 'dim_revenue_per_cus':
            # các cột có trong bảng orders và order_detail

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, '', columns_target, 'dim_revenue_per_cus', 'customer_id')
            print('insert or update table dim_revenue_per_cus successfully')

        if table_name == 'dim_metric':
            # các cột có trong bảng orders và order_detail

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, '', columns_target, 'dim_metric', 'id')
            print('insert or update table fact_orders successfully')

        if table_name == 'dim_date':

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, 'fact_orders', columns_target, 'dim_date', 'order_id')
            # print('insert or update table dim_products successfully')

        if table_name == 'dim_region':

            columns_source = []
            columns_target = []
            update_or_create_db(columns_source, 'customers', columns_target, 'dim_region', 'city')

    except Exception as e:
        print('Error import_data:' + str(e))


def load_dim_customer():
    print('-- Bắt đầu tải bảng dim_customer... --')
    import_data('dim_customer')
    print('-- Hoàn tất tải bảng dim_customer... --')


def load_dim_products():
    print('-- Bắt đầu tải bảng dim_products... --')
    import_data('dim_products')
    print('-- Hoàn tất tải bảng dim_products... --')


def load_dim_revenue_per_cus():
    print('-- Bắt đầu tải bảng dim_revenue_per_cus... --')
    import_data('dim_revenue_per_cus')
    print('-- Hoàn tất tải bảng dim_revenue_per_cus... --')


def load_dim_region_w_customer():
    print('-- Bắt đầu tải bảng dim_region_w_customer... --')
    import_data('dim_region_w_customer')
    print('-- Hoàn tất tải bảng dim_region_w_customer... --')


def load_dim_metric():
    print('-- Bắt đầu tải bảng dim_metric... --')
    import_data('dim_metric')
    print('-- Hoàn tất tải bảng dim_metric... --')


def load_dim_region():
    print('-- Bắt đầu tải bảng dim_region... --')
    import_data('dim_region')
    print('-- Hoàn tất tải bảng dim_region... --')


def load_fact_orders():
    print('-- Bắt đầu tải bảng fact_orders... --')
    import_data('fact_orders')
    print('-- Hoàn tất tải bảng fact_orders... --')


def load_dim_date():
    print('-- Bắt đầu tải bảng dim_date... --')
    import_data('dim_date')
    print('-- Hoàn tất tải bảng dim_date... --')


def init_update_or_create():
    try:
        print('Tiến trình đang chạy...')
        load_dim_customer()
        load_dim_products()
        load_dim_revenue_per_cus()
        load_dim_region_w_customer()
        load_dim_metric()
        load_dim_region()
        load_fact_orders()
        load_dim_date()
        print('Hoàn tất tiến trình.')
        conn.close()
    except Exception as e:
        print('Error:' + str(e))


# init_update_or_create()
conn.close()