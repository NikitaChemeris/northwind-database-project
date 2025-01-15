CREATE OR REPLACE DATABASE northwind_db;
CREATE OR REPLACE STAGE data_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE customers_staging (
    id INT,
    customerName STRING,
    contactName STRING,
    address STRING,
    city STRING,
    postalCode STRING,
    country STRING
);

CREATE OR REPLACE TABLE categories_staging (
    id INT,
    category_name STRING,
    description STRING
);

CREATE OR REPLACE TABLE employees_staging (
    id INT,
    lastName STRING,
    firstName STRING,
    birthDate STRING,
    photo STRING,
    notes STRING
);

CREATE OR REPLACE TABLE shippers_staging (
    id INT,
    shipperName STRING,
    phone STRING
);

CREATE OR REPLACE TABLE suppliers_staging (
    id INT,
    supplierName STRING,
    contactName STRING,
    address STRING,
    city STRING,
    postalCode STRING,
    country STRING,
    phone STRING
);

CREATE OR REPLACE TABLE products_staging (
    id INT,
    productName STRING,
    supplierId INT,
    categoryId INT,
    unit STRING,
    price INT
);

CREATE OR REPLACE TABLE orders_staging (
    id INT,
    customerId INT,
    employeeId INT,
    orderDate STRING,
    shipperId INT
);

CREATE OR REPLACE TABLE order_details_staging (
    id INT,
    orderId INT,
    productId INT,
    quantity INT
);

COPY INTO customers_staging 
FROM @data_stage/Customers.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO categories_staging
FROM @data_stage/Categories.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employees_staging 
FROM @data_stage/Employees.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO shippers_staging 
FROM @data_stage/Shippers.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO suppliers_staging 
FROM @data_stage/Suppliers.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO products_staging
FROM @data_stage/Products.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orders_staging 
FROM @data_stage/Orders.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO order_details_staging
FROM @data_stage/OrderDetails.csv FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

SELECT * FROM customers_staging;
SELECT * FROM categories_staging;
SELECT * FROM employees_staging;
SELECT * FROM shippers_staging;
SELECT * FROM suppliers_staging;
SELECT * FROM products_staging;
SELECT * FROM orders_staging;
SELECT * FROM order_details_staging;

CREATE OR REPLACE TABLE dim_customer AS
SELECT
    id AS customer_id,
    customerName AS customer_name,
    contactName AS contact_name,
    address,
    city,
    postalCode AS postal_code,
    country
FROM customers_staging;

SELECT * FROM dim_customer;

CREATE OR REPLACE TABLE dim_product AS
SELECT
    id AS product_id,
    productName AS product_name,
    supplierId AS supplier_id,
    categoryId AS category_id,
    unit,
    price
FROM products_staging;

SELECT * FROM dim_product;

CREATE OR REPLACE TABLE dim_time AS
SELECT
    DISTINCT LEFT(orderDate, 10) AS order_date,
    EXTRACT(YEAR FROM TO_DATE(LEFT(orderDate, 10), 'YYYY-MM-DD')) AS year,
    EXTRACT(MONTH FROM TO_DATE(LEFT(orderDate, 10), 'YYYY-MM-DD')) AS month,
    EXTRACT(DAY FROM TO_DATE(LEFT(orderDate, 10), 'YYYY-MM-DD')) AS day,
    EXTRACT(QUARTER FROM TO_DATE(LEFT(orderDate, 10), 'YYYY-MM-DD')) AS quarter
FROM orders_staging;

SELECT * FROM dim_time;

CREATE OR REPLACE TABLE dim_supplier AS
SELECT
    id AS supplier_id,
    supplierName AS supplier_name,
    contactName AS contact_name,
    address,
    city,
    postalCode AS postal_code,
    country,
    phone
FROM suppliers_staging;

SELECT * FROM dim_supplier;

CREATE OR REPLACE TABLE dim_category AS
SELECT
    id AS category_id,
    category_name,
    description
FROM categories_staging;

SELECT * FROM dim_category;

CREATE OR REPLACE TABLE sales_fact AS
SELECT
    od.orderId AS order_id,
    o.customerId AS customer_id,
    o.employeeId AS employee_id,
    od.productId AS product_id,
    od.quantity,
    od.quantity * p.price AS total_revenue,
    TO_DATE(LEFT(o.orderDate, 10), 'YYYY-MM-DD') AS order_date
FROM order_details_staging od
    JOIN orders_staging o ON od.orderId = o.id
    JOIN products_staging p ON od.productId = p.id;
    
SELECT * FROM sales_fact;

DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS order_details_staging;
DROP TABLE IF EXISTS categories_staging;