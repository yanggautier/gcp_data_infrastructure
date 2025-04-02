from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime.today(),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=3),
}

BUCKET_NAME = models.Variable.get('bucket_name')
PROJECT_ID = models.Variable.get('project_id')
REGION = models.Variable.get('region')
BIGQUERY_DATASET = models.Variable.get('bigquery_dataset')
DATAFLOW_TEMPLATE_PATH = models.Variable.get('dataflow_template_path')
BUCKET_PREFIX = models.Variable.get('bucket_prefix')



with models.DAG('dataflow_csv_to_bigquery',
                default_args=default_args,
                schedule_interval=None,
                catchup=False,
                tags=['production']) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    for table in ["customer", "geolocalisation", "category_name", "product", "seller", "online_order", "order_item", "order_payment"]:
        dataflow_task = DataflowCreatePythonJobOperator(
            task_id=f'{table}_dataflow_job',
            py_file=DATAFLOW_TEMPLATE_PATH,
            py_interpreter='python3',
            # py_requirements=['apache-beam[gcp]'],
            dataflow_default_options={
                'project': PROJECT_ID,
                'region': REGION,
                'temp_location': f'gs://{BUCKET_NAME}/temp/',
                'staging_location': f'gs://{BUCKET_NAME}/staging/',
                'runner': 'DataflowRunner'
            },
            options={
                'input': f'gs://{BUCKET_NAME}/{BUCKET_PREFIX}/{table}.csv',
                'output': f'{PROJECT_ID}:{BIGQUERY_DATASET}.{table}',
                'table_name': table,
                'project': PROJECT_ID,
                'region': REGION,
            },
            project_id=PROJECT_ID,
            location=REGION,
            dag=dag
        )

        start >> dataflow_task >> end
