variable "credentials" {
  description = "Path to the GCP Service Account JSON file"
  # Update the path below to where your service account JSON file is located.
  default     = "../google_creds/google_credentials.json"  # Adjust path as needed
}

variable "project" {
  description = "GCP Project ID"
  # Update the default value to your Google Cloud project ID
  default     = "<ENTER YOUR PROJECT ID>"  # Replace with your actual project ID
}

variable "region" {
  description = "Region for GCP resources"
  # Update the below to your desired GCP region
  default     = "us-central1" # FEEL FREE TO CHANGE THIS
}

variable "location" {
  description = "Location for BigQuery datasets"
  # Update the below to your desired BigQuery dataset location
  default     = "US" # FEEL FREE TO CHANGE THIS
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  # Update the below to what you want your dataset to be called
  default     = "reddit_data"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage Bucket Name"
  # Update the below to a unique bucket name for storing CSV files and other data
  default     = "<ENTER YOUR BUCKET NAME>"  # Must be globally unique
}

variable "gcs_storage_class" {
  description = "Storage class for the GCS bucket"
  default     = "STANDARD"
}
