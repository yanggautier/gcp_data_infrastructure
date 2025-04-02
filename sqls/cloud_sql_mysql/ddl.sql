USE online_shop;

-- Create the customer table
CREATE TABLE IF NOT EXISTS customer(
    customer_id VARCHAR(50) NOT NULL,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    PRIMARY KEY (customer_id)
);


-- Create the geolocalisation tabe
CREATE TABLE IF NOT EXISTS geolocalisation(
    geolocation_zip_code_prefix INT NOT NULL,
    geolocation_lat DECIMAL(10, 8),
    geolocation_lng DECIMAL(11, 8),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10),
    PRIMARY KEY(geolocation_zip_code_prefix)
);

-- Create the category name table
CREATE TABLE IF NOT EXISTS category_name(
    product_category_name VARCHAR(100) NOT NULL,
    product_category_name_english VARCHAR(100),
    PRIMARY KEY(product_category_name)
);

-- Create the product table
CREATE TABLE IF NOT EXISTS product(
    product_id VARCHAR(50) NOT NULL,
    product_category_name VARCHAR(255) NOT NULL,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    PRIMARY KEY (product_id),
   FOREIGN KEY (product_category_name) REFERENCES category_name(product_category_name)
);

-- Create the seller table
CREATE TABLE IF NOT EXISTS seller(
    seller_id VARCHAR(50) NOT NULL,
    seller_zip_code_prefix INT,
    PRIMARY KEY (seller_id),
    FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocalisation(geolocation_zip_code_prefix)
);

-- Create the order table
CREATE TABLE IF NOT EXISTS online_order(
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

-- Create the order item table
CREATE TABLE IF NOT EXISTS order_item(
    id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    order_id VARCHAR(50) NOT NULL,
    order_item_id SMALLINT,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (seller_id) REFERENCES seller(seller_id),
    FOREIGN KEY (order_id) REFERENCES online_order(order_id)
);

-- Create the order payment table
CREATE TABLE IF NOT EXISTS order_payment(
    id INT NOT NULL AUTO_INCREMENT,
    order_id VARCHAR(50) NOT NULL,
    payment_sequential SMALLINT,
    payment_type VARCHAR(20), 
    payment_installments TINYINT,
    payment_value DECIMAL(10, 2),
    PRIMARY KEY (id),
    FOREIGN KEY (order_id) REFERENCES online_order(order_id)
);

-- Create the order review table
CREATE TABLE IF NOT EXISTS order_review(
    review_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    review_score TINYINT,
    review_comment_title VARCHAR(100), 
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    PRIMARY KEY (review_id),
    FOREIGN KEY (order_id) REFERENCES online_order(order_id)
);