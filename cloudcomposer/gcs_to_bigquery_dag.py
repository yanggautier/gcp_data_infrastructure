import datetime
import logging

from airflow import models
from airflow.contrib.operators import gcs_to_bq
from airflow.operators import dummy_operator

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime.today(),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=3),
}
# Set variables
FILE_LIST = ["customer", "seller", "geolocalisation", "category_name", "product", "online_order", "order_item", "order_payment", "order_review"]
BUCKET_PREFIX = models.Variable.get('bucket_prefix')
BUCKET_NAME = models.Variable.get('bucket_name')
PROJECT_ID = models.Variable.get('project_id')
BIGQUERY_DATASET = models.Variable.get('bigquery_dataset')

# Set GCP logging
logger = logging.getLogger('gcs_to_bigquery')

# Main DAG
with models.DAG('copy_csv_to_bigquery',
                default_args=default_args,
                schedule_interval=None) as dag:
    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    # Loop over each record in the 'all_records' python list to build up
    # Airflow tasks
    for record in FILE_LIST:
        logger.info('Generating tasks to transfer table: {}'.format(record))

        GCS_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id='{}_GCS_to_BQ'.format(record),
            bucket=BUCKET_NAME,
            source_objects=['{}/{}.csv'.format(BUCKET_PREFIX,record)],
            destination_project_dataset_table='{}.{}.{}'.format(PROJECT_ID, BIGQUERY_DATASET, record),
            source_format='CSV',
            skip_leading_rows = 1,
            autodetect=True,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            allow_jagged_rows=True, 
        )

        start >> GCS_to_BQ >> end
