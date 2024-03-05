from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
import pandas as pd
import os

# Assuming these are the correct environment variable keys set in your Docker Compose
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    # Connect to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    
    # Extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    
    # Transformation
    post_df = pd.DataFrame(posts)
    post_df = transform_data(post_df)
    
    # Loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)
    
    # Upload to GCS
    object_name = f'reddit_data/{file_name}.csv'  # Adjust path as needed
    upload_to_gcs(BUCKET, object_name, file_path)

# This function should be defined in your etls.reddit_etl module or a similar utility module
def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    """Uploads a file to the bucket."""
    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")