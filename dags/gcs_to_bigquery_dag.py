import datetime
import logging

from airflow import models
from airflow.providers.google.cloud.transfers import gcs_to_bigquery
from airflow.operators import dummy_operator

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime.today(),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=3),
}
# Set variables
BUCKET_PREFIX = models.Variable.get('bucket_prefix')
BUCKET_NAME = models.Variable.get('bucket_name')
PROJECT_ID = models.Variable.get('project_id')
BIGQUERY_DATASET = models.Variable.get('bigquery_dataset')

# Set GCP logging
logger = logging.getLogger('gcs_to_bigquery')

SCHEMA_MAPPING = {
    "customer": [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_unique_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_zip_code_prefix", "type": "INTEGER"},
        {"name": "customer_city", "type": "STRING"},
        {"name": "customer_state", "type": "STRING"}
    ],
    "geolocalisation": [
        {"name": "geolocation_zip_code_prefix", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "geolocation_lat", "type": "FLOAT"},
        {"name": "geolocation_lng", "type": "FLOAT"},
        {"name": "geolocation_city", "type": "STRING"},
        {"name": "geolocation_state", "type": "STRING"}
    ],
    "category_name": [
        {"name": "product_category_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_category_name_english", "type": "STRING"}
    ],
    "product": [
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_category_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name_lenght", "type": "INTEGER"},
        {"name": "product_description_lenght", "type": "INTEGER"},
        {"name": "product_photos_qty", "type": "INTEGER"},
        {"name": "product_weight_g", "type": "FLOAT"},
        {"name": "product_length_cm", "type": "FLOAT"},
        {"name": "product_height_cm", "type": "FLOAT"},
        {"name": "product_width_cm", "type": "FLOAT"}
    ],
    "seller": [
        {"name": "seller_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "seller_zip_code_prefix", "type": "INTEGER"}
    ],
    "online_order": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_status", "type": "STRING"},
        {"name": "order_purchase_timestamp", "type": "STRING"},
        {"name": "order_approved_at", "type": "TIMESTAMP"},
        {"name": "order_delivered_carrier_date", "type": "STRING"},
        {"name": "order_delivered_customer_date", "type": "STRING"},
        {"name": "order_estimated_delivery_date", "type": "STRING"}
    ],
    "order_item": [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_item_id", "type": "INTEGER"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "seller_id", "type": "STRING"},
        {"name": "shipping_limit_date", "type": "STRING"},
        {"name": "price", "type": "FLOAT"},
        {"name": "freight_value", "type": "FLOAT"}
    ],
    "order_payment": [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "payment_sequential", "type": "INTEGER"},
        {"name": "payment_type", "type": "STRING"},
        {"name": "payment_installments", "type": "INTEGER"},
        {"name": "payment_value", "type": "FLOAT"}
    ],
    "order_review": [
        {"name": "review_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "review_score", "type": "INTEGER"},
        {"name": "review_comment_title", "type": "STRING"},
        {"name": "review_comment_message", "type": "STRING"},
        {"name": "review_creation_date", "type": "STRING"},
        {"name": "review_answer_timestamp", "type": "STRING"}
    ]
}

# Main DAG
with models.DAG('copy_csv_to_bigquery',
                default_args=default_args,
                schedule_interval=None,
                catchup=False,
                tags=['production']
) as dag:
    
    start = dummy_operator.DummyOperator(task_id='start')
    end = dummy_operator.DummyOperator(task_id='end')

    # Airflow tasks
    for record in SCHEMA_MAPPING:
        transfer_task = gcs_to_bigquery.GCSToBigQueryOperator(
            task_id=f'{record}_gcs_to_bq',
            bucket=BUCKET_NAME,
            source_objects=[f'{BUCKET_PREFIX}/{record}.csv'],
            destination_project_dataset_table=f'{PROJECT_ID}.{BIGQUERY_DATASET}.{record}',
            source_format='CSV',
            max_bad_records=100, 
            field_delimiter=',',
            quote_character='"',
            skip_leading_rows=0,
            autodetect=False,
            schema_fields=SCHEMA_MAPPING[record],  # Utilisation du schéma approprié
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            dag=dag
        )

        start >> transfer_task >> end

