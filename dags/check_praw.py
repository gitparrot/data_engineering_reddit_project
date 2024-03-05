from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

def comprehensive_praw_check():
    # Check if praw is installed
    print("Checking for praw installation...")
    try:
        import praw
        print(f"praw is installed. Version: {praw.__version__}")
    except ImportError as e:
        print("praw is not installed.")
        sys.exit(1)
    
    # Print PYTHONPATH
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}")
    
    # Print sys.path
    print("sys.path contents:")
    for path in sys.path:
        print(path)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 4),
}

with DAG('comprehensive_praw_check_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    praw_check = PythonOperator(
        task_id='praw_check',
        python_callable=comprehensive_praw_check,
    )

praw_check
