USE online_shop;


-- Create tempary tables
CREATE TEMPORARY TABLE temp_customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

CREATE TEMPORARY TABLE temp_geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(10, 8),
    geolocation_lng DECIMAL(11, 8),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10)
);

CREATE TEMPORARY TABLE temp_order_items (
    order_id VARCHAR(50),
    order_item_id SMALLINT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
);

CREATE TEMPORARY TABLE temp_order_payments (
    order_id VARCHAR(50),
    payment_sequential SMALLINT,
    payment_type VARCHAR(20),
    payment_installments TINYINT,
    payment_value DECIMAL(10, 2)
);

CREATE TEMPORARY TABLE temp_order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score TINYINT,
    review_comment_title VARCHAR(100),
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME
);

CREATE TEMPORARY TABLE temp_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);

CREATE TEMPORARY TABLE temp_products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(255),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g DOUBLE,
    product_length_cm DOUBLE,
    product_height_cm DOUBLE,
    product_width_cm DOUBLE
);

CREATE TEMPORARY TABLE temp_sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

CREATE TEMPORARY TABLE temp_category_name (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);


-- Load file into temporary tables
LOAD DATA INFILE '/private/var/lib/mysql-files/olist_customers_dataset.csv'
INTO TABLE temp_customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/private/var/lib/mysql-files/olist_geolocation_dataset.csv'
INTO TABLE temp_geolocation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/private/var/lib/mysql-files/olist_order_items_dataset.csv'
INTO TABLE temp_order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/private/var/lib/mysql-files/olist_order_payments_dataset.csv'
INTO TABLE temp_order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/private/var/lib/mysql-files/olist_order_reviews_dataset.csv'
INTO TABLE temp_order_reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA INFILE '/private/var/lib/mysql-files/olist_orders_dataset.csv' 
INTO TABLE temp_orders 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(
    order_id,
    customer_id,
    order_status,
    @order_purchase_timestamp,
    @order_approved_at,
    @order_delivered_carrier_date,
    @order_delivered_customer_date,
    @order_estimated_delivery_date
)
SET 
    order_purchase_timestamp = NULLIF(@order_purchase_timestamp, ''),
    order_approved_at = NULLIF(@order_approved_at, ''),
    order_delivered_carrier_date = NULLIF(@order_delivered_carrier_date, ''),
    order_delivered_customer_date = NULLIF(@order_delivered_customer_date, ''),
    order_estimated_delivery_date = NULLIF(@order_estimated_delivery_date, '');
    

LOAD DATA INFILE '/private/var/lib/mysql-files/olist_products_dataset.csv'
INTO TABLE temp_products 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(
    product_id,
    product_category_name,
    @product_name_lenght,
    @product_description_lenght,
    @product_photos_qty,
    @product_weight_g,
    @product_length_cm,
    @product_height_cm,
    @product_width_cm
)
SET 
    product_name_lenght = NULLIF(@product_name_lenght, ''),
    product_description_lenght = NULLIF(@product_description_lenght, ''),
    product_photos_qty = NULLIF(@product_photos_qty, ''),
    product_weight_g = NULLIF(@product_weight_g, ''),
    product_length_cm = NULLIF(@product_length_cm, ''),
    product_height_cm = NULLIF(@product_height_cm, ''),
    product_width_cm = NULLIF(@product_width_cm, '');
    

LOAD DATA INFILE '/private/var/lib/mysql-files/olist_sellers_dataset.csv'
INTO TABLE temp_sellers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/private/var/lib/mysql-files/product_category_name_translation.csv'
INTO TABLE temp_category_name
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- Insert into final tables
INSERT INTO customer (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM temp_customers;

INSERT INTO geolocalisation (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
FROM temp_geolocation;


INSERT INTO online_order (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
SELECT order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
FROM temp_orders;

INSERT INTO category_name(product_category_name, product_category_name_english)
SELECT product_category_name, product_category_name_english
FROM temp_category_name;

INSERT INTO category_name(product_category_name, product_category_name_english)
SELECT DISTINCT(product_category_name), NULL
FROM temp_products
WHERE product_category_name NOT IN (
    SELECT DISTINCT(product_category_name)
    FROM category_name
);

INSERT INTO product (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
SELECT product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
FROM temp_products;


INSERT INTO geolocalisation(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
SELECT DISTINCT(seller_zip_code_prefix), NULL, NULL, seller_city, seller_state
FROM temp_sellers
WHERE seller_zip_code_prefix NOT IN (
    SELECT DISTINCT(geolocation_zip_code_prefix)
    FROM geolocalisation
);

INSERT INTO seller (seller_id, seller_zip_code_prefix)
SELECT seller_id, seller_zip_code_prefix
FROM temp_sellers;


INSERT INTO order_item (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
SELECT order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
FROM temp_order_items;


INSERT INTO order_payment (order_id, payment_sequential, payment_type, payment_installments, payment_value)
SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
FROM temp_order_payments;

INSERT INTO order_review (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
SELECT review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp
FROM temp_order_reviews;


-- Delete temporary tables
DROP TEMPORARY TABLE temp_customers;
DROP TEMPORARY TABLE temp_geolocation;
DROP TEMPORARY TABLE temp_order_items;
DROP TEMPORARY TABLE temp_order_payments;
DROP TEMPORARY TABLE temp_order_reviews;
DROP TEMPORARY TABLE temp_orders;
DROP TEMPORARY TABLE temp_products;
DROP TEMPORARY TABLE temp_sellers;
DROP TEMPORARY TABLE temp_category_name;
