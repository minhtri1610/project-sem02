-- tạo một schema cứ một data mart ý tưởng sẽ là 1 schema thay vì sử dụng schema mặc định là public
CREATE SCHEMA IF NOT EXISTS datamart_customer;

-- tạo bảng dim
CREATE TABLE IF NOT EXISTS datamart_customer.dim_customer (
    customer_id bpchar NOT NULL PRIMARY KEY,
    company_name character varying(40) NOT NULL,
    contact_name character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15)
);

CREATE TABLE IF NOT EXISTS datamart_customer.dim_products (
    product_id int NOT NULL PRIMARY KEY,
    product_name character varying(40) NOT NULL,
    quantity_per_unit character varying(20),
    unit_price real,
    units_in_stock smallint,
    units_on_order smallint,
    reorder_level smallint,
    discontinued integer NOT NULL
);

CREATE TABLE IF NOT EXISTS datamart_customer.dim_employees (
    employee_id int NOT NULL PRIMARY KEY,
    last_name character varying(20),
    first_name character varying(10),
    title character varying(30),
    city character varying(15),
    region character varying(15),
    country character varying(15),
    reports_to smallint
);


-- tạo bảng fact
CREATE TABLE datamart_customer.fact_orders (
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

CREATE TABLE IF NOT EXISTS datamart_customer.dim_date (
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