-- Création des tables de dimensions

-- Dimension Client (DIM_CUSTOMER)
CREATE  OR REPLACE TABLE `star_model.DIM_CUSTOMER` (
    customer_key INT64 NOT NULL,
    customer_id STRING NOT NULL,
    customer_unique_id STRING NOT NULL,
    customer_zip_code_prefix INT64 NOT NULL,
    customer_city STRING,
    customer_state STRING
);

-- Dimension Vendeur (DIM_SELLER)
CREATE  OR REPLACE TABLE`star_model.DIM_SELLER` (
    seller_key INT64 NOT NULL,
    seller_id STRING NOT NULL,
    seller_zip_code_prefix INT64,
    seller_city STRING,
    seller_state STRING
);

-- Dimension Produit (DIM_PRODUCT)
CREATE  OR REPLACE TABLE `star_model.DIM_PRODUCT` (
    product_key INT64 NOT NULL,
    product_id STRING NOT NULL,
    product_category_name STRING,
    product_category_name_english STRING,
    product_name_length INT64,
    product_description_length INT64,
    product_photos_qty INT64,
    product_weight_g FLOAT64,
    product_length_cm FLOAT64,
    product_height_cm FLOAT64,
    product_width_cm FLOAT64
);

-- Dimension Date (DIM_DATE)
CREATE  OR REPLACE TABLE `star_model.DIM_DATE` (
    date_key INT64 NOT NULL, -- Format YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INT64,
    day_of_month INT64,
    day_of_year INT64,
    week_of_year INT64,
    month INT64,
    month_name STRING,
    quarter INT64,
    year INT64,
    is_weekend BOOL
);

-- Dimension Statut de Commande (DIM_ORDER_STATUS)
CREATE  OR REPLACE TABLE `star_model.DIM_ORDER_STATUS` (
    status_key INT64 NOT NULL,
    order_status STRING NOT NULL
);

-- Dimension Méthode de Paiement (DIM_PAYMENT_TYPE)
CREATE  OR REPLACE TABLE `star_model.DIM_PAYMENT_TYPE` (
    payment_type_key INT64 NOT NULL,
    payment_type STRING NOT NULL
);

-- Dimension Géolocalisation (DIM_GEOLOCATION)
CREATE  OR REPLACE TABLE `star_model.DIM_GEOLOCATION` (
    geolocation_key INT64 NOT NULL,
    zip_code_prefix INT64 NOT NULL,
    latitude FLOAT64,
    longitude FLOAT64,
    city STRING,
    state STRING
);

-- Création de la table de faits principale (FACT_ORDERS)
CREATE  OR REPLACE TABLE `star_model.FACT_ORDERS` (
    order_fact_key INT64 NOT NULL,
    order_id STRING NOT NULL,
    customer_key INT64 NOT NULL,
    seller_key INT64 NOT NULL,
    product_key INT64 NOT NULL,
    status_key INT64 NOT NULL,
    payment_type_key INT64 NOT NULL,
    purchase_date_key INT64 NOT NULL,
    approved_date_key INT64,
    carrier_date_key INT64,
    delivered_date_key INT64,
    estimated_date_key INT64,
    order_item_id INT64 NOT NULL,
    shipping_limit_date_key INT64,
    price NUMERIC(8,2) NOT NULL,
    freight_value NUMERIC(8,2) NOT NULL,
    payment_value NUMERIC(8,2) NOT NULL,
    payment_installments INT64,
    payment_sequential INT64
);

-- Peuplement de DIM_CUSTOMER
INSERT INTO `star_model.DIM_CUSTOMER` (customer_key, customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
SELECT 
    ROW_NUMBER() OVER() as customer_key,
    customer_id, 
    customer_unique_id, 
    customer_zip_code_prefix, 
    customer_city, 
    customer_state
FROM (
    SELECT DISTINCT 
        customer_id, 
        customer_unique_id, 
        customer_zip_code_prefix, 
        customer_city, 
        customer_state
    FROM `raw_dataset.customer`
);

-- Peuplement de DIM_SELLER
INSERT INTO `star_model.DIM_SELLER` (seller_key, seller_id, seller_zip_code_prefix, seller_city, seller_state)
SELECT 
    ROW_NUMBER() OVER() as seller_key,
    seller_id, 
    seller_zip_code_prefix, 
    seller_city, 
    seller_state
FROM (
    SELECT DISTINCT 
        seller_id, 
        seller_zip_code_prefix, 
        seller_city, 
        seller_state
    FROM `raw_dataset.seller`
);

-- Peuplement de DIM_PRODUCT avec traduction des catégories
INSERT INTO `star_model.DIM_PRODUCT` (
    product_key,
    product_id, 
    product_category_name, 
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT 
    ROW_NUMBER() OVER() as product_key,
    p.product_id, 
    p.product_category_name, 
    t.product_category_name_english,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM 
    `raw_dataset.product` p
LEFT JOIN 
    `raw_dataset.category_name` t ON p.product_category_name = t.product_category_name;

-- Peuplement de DIM_ORDER_STATUS
INSERT INTO `star_model.DIM_ORDER_STATUS` (status_key, order_status)
SELECT 
    ROW_NUMBER() OVER() as status_key,
    order_status
FROM (
    SELECT DISTINCT order_status
    FROM `raw_dataset.online_order`
);

-- Peuplement de DIM_PAYMENT_TYPE
INSERT INTO `star_model.DIM_PAYMENT_TYPE` (payment_type_key, payment_type)
SELECT 
    ROW_NUMBER() OVER() as payment_type_key,
    payment_type
FROM (
    SELECT DISTINCT payment_type
    FROM `raw_dataset.order_payment`
);

-- Peuplement de DIM_GEOLOCATION
INSERT INTO `star_model.DIM_GEOLOCATION` (geolocation_key, zip_code_prefix, latitude, longitude, city, state)
SELECT 
    ROW_NUMBER() OVER() as geolocation_key,
    geolocation_zip_code_prefix, 
    geolocation_lat, 
    geolocation_lng, 
    geolocation_city, 
    geolocation_state
FROM (
    SELECT DISTINCT 
        geolocation_zip_code_prefix, 
        geolocation_lat, 
        geolocation_lng, 
        geolocation_city, 
        geolocation_state
    FROM `raw_dataset.geolocalisation`
);

-- Script pour peupler DIM_DATE (en utilisant la fonction GENERATE_DATE_ARRAY de BigQuery)
INSERT INTO `star_model.DIM_DATE` (
    date_key,
    full_date,
    day_of_week,
    day_of_month,
    day_of_year,
    week_of_year,
    month,
    month_name,
    quarter,
    year,
    is_weekend
)
SELECT
    EXTRACT(YEAR FROM d) * 10000 + EXTRACT(MONTH FROM d) * 100 + EXTRACT(DAY FROM d) as date_key,
    d as full_date,
    EXTRACT(DAYOFWEEK FROM d) as day_of_week,
    EXTRACT(DAY FROM d) as day_of_month,
    EXTRACT(DAYOFYEAR FROM d) as day_of_year,
    EXTRACT(WEEK FROM d) as week_of_year,
    EXTRACT(MONTH FROM d) as month,
    FORMAT_DATE('%B', d) as month_name,
    EXTRACT(QUARTER FROM d) as quarter,
    EXTRACT(YEAR FROM d) as year,
    EXTRACT(DAYOFWEEK FROM d) IN (1, 7) as is_weekend
FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', '2020-12-31')) as d;

-- Finalement, peuplement de la table de faits FACT_ORDERS
INSERT INTO `star_model.FACT_ORDERS` (
    order_fact_key,
    order_id,
    customer_key,
    seller_key,
    product_key,
    status_key,
    payment_type_key,
    purchase_date_key,
    approved_date_key,
    carrier_date_key,
    delivered_date_key,
    estimated_date_key,
    order_item_id,
    shipping_limit_date_key,
    price,
    freight_value,
    payment_value,
    payment_installments,
    payment_sequential
)
SELECT 
    ROW_NUMBER() OVER() as order_fact_key,
    oi.order_id,
    dc.customer_key,
    ds.seller_key,
    dp.product_key,
    dos.status_key,
    dpt.payment_type_key,
    EXTRACT(YEAR FROM o.order_purchase_timestamp) * 10000 + EXTRACT(MONTH FROM o.order_purchase_timestamp) * 100 + EXTRACT(DAY FROM o.order_purchase_timestamp) as purchase_date_key,
    CASE WHEN o.order_approved_at IS NOT NULL THEN 
        EXTRACT(YEAR FROM o.order_approved_at) * 10000 + EXTRACT(MONTH FROM o.order_approved_at) * 100 + EXTRACT(DAY FROM o.order_approved_at) 
    ELSE NULL END as approved_date_key,
    CASE WHEN o.order_delivered_carrier_date IS NOT NULL THEN 
        EXTRACT(YEAR FROM o.order_delivered_carrier_date) * 10000 + EXTRACT(MONTH FROM o.order_delivered_carrier_date) * 100 + EXTRACT(DAY FROM o.order_delivered_carrier_date) 
    ELSE NULL END as carrier_date_key,
    CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN 
        EXTRACT(YEAR FROM o.order_delivered_customer_date) * 10000 + EXTRACT(MONTH FROM o.order_delivered_customer_date) * 100 + EXTRACT(DAY FROM o.order_delivered_customer_date) 
    ELSE NULL END as delivered_date_key,
    EXTRACT(YEAR FROM o.order_estimated_delivery_date) * 10000 + EXTRACT(MONTH FROM o.order_estimated_delivery_date) * 100 + EXTRACT(DAY FROM o.order_estimated_delivery_date) as estimated_date_key,
    CAST(oi.order_item_id AS INT64) as order_item_id,
    EXTRACT(YEAR FROM oi.shipping_limit_date) * 10000 + EXTRACT(MONTH FROM oi.shipping_limit_date) * 100 + EXTRACT(DAY FROM oi.shipping_limit_date) as shipping_limit_date_key,
    CAST(oi.price AS NUMERIC) as price,
    CAST(oi.freight_value AS NUMERIC) as freight_value,
    CAST(op.payment_value AS NUMERIC) as payment_value,
    op.payment_installments,
    op.payment_sequential
FROM 
    `raw_dataset.order_item` oi
JOIN 
    `raw_dataset.online_order` o ON oi.order_id = o.order_id
JOIN 
    `raw_dataset.order_payment` op ON o.order_id = op.order_id
JOIN 
    `star_model.DIM_CUSTOMER` dc ON o.customer_id = dc.customer_id
JOIN 
    `star_model.DIM_SELLER` ds ON oi.seller_id = ds.seller_id
JOIN 
    `star_model.DIM_PRODUCT` dp ON oi.product_id = dp.product_id
JOIN 
    `star_model.DIM_ORDER_STATUS` dos ON o.order_status = dos.order_status
JOIN 
    `star_model.DIM_PAYMENT_TYPE` dpt ON op.payment_type = dpt.payment_type;

-- Création de vues matérialisées pour optimiser les requêtes fréquentes
CREATE MATERIALIZED VIEW `star_model.monthly_sales_by_category` AS
SELECT
    d.year,
    d.month,
    d.month_name,
    p.product_category_name_english,
    COUNT(f.order_id) AS number_of_orders,
    SUM(f.price) AS total_sales,
    AVG(f.price) AS average_order_value
FROM 
    `star_model.FACT_ORDERS` f
JOIN 
    `star_model.DIM_DATE` d ON f.purchase_date_key = d.date_key
JOIN 
    `star_model.DIM_PRODUCT` p ON f.product_key = p.product_key
GROUP BY 
    d.year, d.month, d.month_name, p.product_category_name_english;