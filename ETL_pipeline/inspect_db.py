from sqlalchemy import create_engine, inspect

# PG info
pg_user = 'postgres'
pg_pwd = 'admin12345'
pg_server = '103.130.215.192'
pg_db = 'northwind_v4'

engine = create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(pg_user, pg_pwd, pg_server, '5432', pg_db))

# Tạo đối tượng inspector để khám phá cấu trúc cơ sở dữ liệu
inspector = inspect(engine)

# Lấy danh sách các bảng trong cơ sở dữ liệu
table_names = inspector.get_table_names()

# Duyệt qua từng bảng để lấy thông tin Foreign Key
for table_name in table_names:
    # Lấy danh sách các Foreign Key cho bảng hiện tại
    print(table_name)
    foreign_keys = inspector.get_foreign_keys(table_name)
    print(foreign_keys)
    # In ra thông tin về Foreign Key
    for foreign_key in foreign_keys:
        print(f"Table: {table_name}")
        print(f"Constraint Name: {foreign_key['name']}")
        print(f"Source Columns: {foreign_key['constrained_columns']}")
        print(f"Referenced Table: {foreign_key['referred_table']}")
        print(f"Referenced Columns: {foreign_key['referred_columns']}")
        print("---------------------")

# Đóng kết nối tới cơ sở dữ liệu
engine.dispose()