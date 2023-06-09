-- tạo một schema cứ một data mart ý tưởng sẽ là 1 schema thay vì sử dụng schema mặc định là public
CREATE SCHEMA datamart_customer;

-- tạo bảng dim
CREATE TABLE datamart_customer.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100)
);

CREATE TABLE datamart_customer.dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    product_category VARCHAR(100),
    product_subcategory VARCHAR(100)
);

CREATE TABLE datamart_customer.dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE,
    year INT,
    month INT,
    day INT
);

-- tạo bảng fact
CREATE TABLE datamart_customer.fact_sales (
    sales_id SERIAL PRIMARY KEY,
    customer_id INT,
    product_id INT,
    date_id INT,
    quantity INT,
    amount DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES datamart_customer.dim_customer (customer_id),
    FOREIGN KEY (product_id) REFERENCES datamart_customer.dim_product (product_id),
    FOREIGN KEY (date_id) REFERENCES datamart_customer.dim_date (date_id)
);

-- insert dữ liệu vào bảng dim
INSERT INTO datamart_customer.dim_customer (customer_name, customer_city, customer_state)
VALUES ('John Doe', 'New York', 'NY'),
       ('Jane Smith', 'Los Angeles', 'CA');

INSERT INTO datamart_customer.dim_product (product_name, product_category, product_subcategory)
VALUES ('Widget A', 'Electronics', 'Gadgets'),
       ('Widget B', 'Electronics', 'Gadgets');

INSERT INTO datamart_customer.dim_date (date_value, year, month, day)
VALUES ('2023-01-01', 2023, 1, 1),
       ('2023-01-02', 2023, 1, 2);

-- điền dữ liệu vào bảng fact
INSERT INTO datamart_customer.fact_sales (customer_id, product_id, date_id, quantity, amount)
VALUES (1, 1, 1, 10, 100.00),
       (2, 2, 2, 5, 50.00);

-- truy vấn từ datamarts

