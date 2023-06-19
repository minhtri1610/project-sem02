-- tạo một schema cứ một data mart ý tưởng sẽ là 1 schema thay vì sử dụng schema mặc định là public
CREATE SCHEMA datamart_customer;

-- tạo bảng dim
CREATE TABLE datamart_customer.dim_customer (
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

CREATE TABLE datamart_customer.dim_products (
    product_id smallint NOT NULL PRIMARY KEY,
    product_name character varying(40) NOT NULL,
    quantity_per_unit character varying(20),
    unit_price real,
    units_in_stock smallint,
    units_on_order smallint,
    reorder_level smallint,
    discontinued integer NOT NULL
);

CREATE TABLE datamart_customer.dim_date (
    order_date date,
    year INT,
    month INT,
    day INT,
    short_name text,
    Quarter INT
);

-- tạo bảng fact
CREATE TABLE datamart_customer.fact_orders (
    order_id smallint NOT NULL PRIMARY KEY,
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