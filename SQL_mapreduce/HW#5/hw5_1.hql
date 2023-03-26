USE dualcore;

DROP TABLE IF EXISTS loyalty_program;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_details;

CREATE TABLE loyalty_program
(
    id INT,
    f_name STRING,
    l_name STRING,
    email  STRING,
    loyal  STRING,
    phone  MAP<STRING, STRING>,
    oders  ARRAY<INT>,
    summarize STRUCT<min: INT,
                    max: INT,
                    ave: DOUBLE,
                    total: INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

CREATE TABLE customers
(
    cust_id STRING,
    fname STRING,
    lname STRING,
    address STRING,
    city STRING,
    state STRING,
    zipcode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dualcore.db/customers';

CREATE TABLE products
(
    prod_id INT,
    brand   STRING,
    name    STRING,
    price   INT,
    cost    INT,
    shipping_wt INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dualcore.db/products';

CREATE TABLE orders
(
    order_id INT,
    cust_id  INT,
    order_date TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dualcore.db/orders';

CREATE TABLE order_details
(
    order_id INT,
    prod_id  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dualcore.db/order_details';