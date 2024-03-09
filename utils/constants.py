import configparser
import os

# Create a configparser object
parser = configparser.ConfigParser()

# Read the configuration file
config_file_path = os.path.join(os.path.dirname(__file__), '../config/config.conf')
parser.read(config_file_path)

# Retrieve API keys
SECRET = parser.get('api_keys', 'reddit_secret_key')
CLIENT_ID = parser.get('api_keys', 'reddit_client_id')

# Retrieve database settings
DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_USERNAME = parser.get('database', 'database_username')
DATABASE_PASSWORD = parser.get('database', 'database_password')

# Retrieve file paths
INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

# Retrieve ETL settings
BATCH_SIZE = parser.getint('etl_settings', 'batch_size')  # Use getint for integer
LOG_LEVEL = parser.get('etl_settings', 'log_level')

POST_FIELDS = (
    'id',
    'title',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'upvote_ratio',
    'locked',
    'removal_reason',
    'report_reasons',
    'removed_by',
    'mod_reason_by'
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")