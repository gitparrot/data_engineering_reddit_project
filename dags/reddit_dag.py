from airflow import DAG
from datetime import datetime
import os
import sys
import logging

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import gzip
import pandas as pd


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.reddit_pipeline import reddit_pipeline, upload_to_gcs, load_data_to_bigquery, create_dataset_if_not_exists, remove_duplicates
import utils.constants

# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")



default_args ={
    'owner': 'Airflow User',
    'start_date': datetime(year=2024, month=3, day=5),
}

file_postfix = datetime.now().strftime("%Y%m%d")

with DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id = 'reddit_extraction',
        python_callable=reddit_pipeline,
        op_kwargs = {
            'file_name': f'reddit_{file_postfix}',
            'subreddit': 'worldnews',
            'time_filter': 'hour',
            'limit': 100000
        }
    )

    gcs_load = PythonOperator(
        task_id='gcs_loading',
        python_callable=upload_to_gcs,
        op_kwargs = {
            'bucket_name':utils.constants.BUCKET, 
            'destination_blob_name': f'reddit_data/reddit_{file_postfix}.csv', 
            'source_file_name': f'{utils.constants.OUTPUT_PATH}/reddit_{file_postfix}.csv' 
        }, 
        
    )

    bigquery_load = PythonOperator(
        task_id='bq_loading',
        python_callable=load_data_to_bigquery,
        op_kwargs = {
            'bucket_name':utils.constants.BUCKET, 
            'source_file_name': f'reddit_data/reddit_{file_postfix}.csv', 
            'project_id':utils.constants.PROJECT_ID, 
            'dataset_id': 'reddit_data', 
            'table_id': 'worldnews_data'
        },
       
    )

    # Assuming file_postfix and OUTPUT_PATH are defined and accessible globally within your DAG file
    delete_csv = BashOperator(
        task_id='delete_csv_file',
        bash_command=f'rm -f {utils.constants.OUTPUT_PATH}/reddit_{file_postfix}.csv',
    )

    deduplicate_data = PythonOperator(
        task_id='deduplicate_bigquery_data',
        python_callable=remove_duplicates,
        op_kwargs={
            'project_id': utils.constants.PROJECT_ID,
            'dataset_id': 'reddit_data',
            'table_id': 'worldnews_data',
        },
    )

    export_bigquery_to_gcs = BigQueryToGCSOperator(
        task_id='export_bigquery_to_gcs',
        source_project_dataset_table=f'{utils.constants.PROJECT_ID}.reddit_data.worldnews_data',
        destination_cloud_storage_uris=[f'gs://{utils.constants.BUCKET}/reddit_bq/reddit_data_export_{file_postfix}.csv'],
        export_format='CSV',
        field_delimiter=',',
        print_header=True
    )


extract >> gcs_load >> bigquery_load >> delete_csv >> deduplicate_data >> export_bigquery_to_gcs