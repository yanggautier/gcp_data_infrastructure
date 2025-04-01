from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.operators.dummy_operator import DummyOperator
import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime.today(),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=3),
}

BUCKET_NAME = models.Variable.get('bucket_name')
PROJECT_ID = models.Variable.get('project_id')
BIGQUERY_DATASET = models.Variable.get('bigquery_dataset')
DATAFLOW_TEMPLATE_PATH = f"gs://{BUCKET_NAME}/dataflow/templates/csv_to_bq"

with models.DAG('dataflow_csv_to_bigquery',
                default_args=default_args,
                schedule_interval=None,
                catchup=False,
                tags=['production']) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    for table in ["customer", "geolocalisation", "category_name", "product", "seller", "online_order", "order_item", "order_payment", "order_review"]:
        dataflow_task = DataflowTemplatedJobStartOperator(
            task_id=f'{table}_dataflow_job',
            template=DATAFLOW_TEMPLATE_PATH,
            parameters={
                'input': f'gs://{BUCKET_NAME}/raw/{table}.csv',
                'output': f'{PROJECT_ID}:{BIGQUERY_DATASET}.{table}',
                'table_name': table  # Passer le nom de la table pour mapper les colonnes
            },
            project_id=PROJECT_ID,
            location='europe-west9',
            dag=dag
        )

        start >> dataflow_task >> end
