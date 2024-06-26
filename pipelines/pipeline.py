from etls.etl import connect_api, post_extraction, data_transform, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
import pandas as pd
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Assuming these are the correct environment variable keys set in your Docker Compose
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def create_dataset_if_not_exists(client, dataset_id):
    """Creates a BigQuery dataset if it does not exist."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Choose the appropriate location
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

def worldnews_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    # Connect to reddit instance
    instance = connect_api(CLIENT_ID, SECRET, 'Umar Agent')
    
    # Extraction
    posts = post_extraction(instance, subreddit, time_filter, limit)
    
    # Transformation
    post_dataframe = pd.DataFrame(posts)
    post_dataframe = data_transform(post_dataframe)
    
    # Loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_dataframe, file_path)
    
    # # Upload to GCS
    # object_name = f'reddit_data/{file_name}.csv'  # Adjust path as needed
    # upload_to_gcs(BUCKET, object_name, file_path)

    # # Load data to BigQuery
    # dataset_id = 'reddit_data'  # Dataset ID
    # table_id = 'worldnews_data'  # Table ID
    # load_data_to_bigquery(BUCKET, object_name, PROJECT_ID, dataset_id, table_id)

def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    """Uploads a file to the bucket."""
    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def load_data_to_bigquery(bucket_name, source_file_name, project_id, dataset_id, table_id):
    """Loads the CSV data from GCS to BigQuery, creating the dataset and table if they do not exist.
    The table is partitioned by date and clustered by author and score."""
    client = bigquery.Client(project=project_id)
    create_dataset_if_not_exists(client, dataset_id)

    table_ref = bigquery.TableReference.from_string(f"{project_id}.{dataset_id}.{table_id}")
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.skip_leading_rows = 1
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    uri = f"gs://{bucket_name}/{source_file_name}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

    print(f"Starting job {load_job.job_id}")
    load_job.result()  # Waits for the job to complete.
    print(f"Job finished.")

    destination_table = client.get_table(table_ref)
    print(f"Loaded {destination_table.num_rows} rows.")

def remove_duplicates(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig()

    deduplication_query = f"""
    DELETE FROM `{project_id}.{dataset_id}.{table_id}` t1
    WHERE EXISTS (
        SELECT 1
        FROM `{project_id}.{dataset_id}.{table_id}` t2
        WHERE t1.id = t2.id
        AND t1.created_utc < t2.created_utc
    );
    """

    query_job = client.query(deduplication_query, job_config=job_config)
    query_job.result()  # Wait for the query to finish
    print(f"Deduplication complete for table {table_id}")
    
    # Run the query
    query_job = client.query(deduplication_query, job_config=job_config)
    query_job.result()  # Wait for the query to finish
    print(f"Deduplication complete for table {table_id}")

