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
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import gzip
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

sys.path.insert(__index: 0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args ={
    'owner': 'Airflow User',
    'start_date': datetime(year: 2024, month: 3, day: 4)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedul_interval='@daily',
    catchup=False
)

extract = PythonOperator(
    task_id = 'reddit_extract',
    python_callable=reddit_pipeline,
    op_kwargs = {
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'worldnews',
        'time_filter': 'day'
        'limit': 100
    }
)