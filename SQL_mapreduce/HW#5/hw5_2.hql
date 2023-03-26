USE dualcore;

LOAD DATA LOCAL INPATH '/hadoop-data/loyalty_data.txt' OVERWRITE
INTO TABLE loyalty_program;

LOAD DATA LOCAL INPATH '/hadoop-data/customers' OVERWRITE
INTO TABLE customers;

LOAD DATA LOCAL INPATH '/hadoop-data/orders' OVERWRITE
INTO TABLE orders;

LOAD DATA LOCAL INPATH '/hadoop-data/order_details' OVERWRITE
INTO TABLE order_details;

LOAD DATA LOCAL INPATH '/hadoop-data/products' OVERWRITE
INTO TABLE products;