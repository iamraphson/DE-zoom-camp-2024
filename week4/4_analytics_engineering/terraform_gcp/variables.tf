locals {
  DEZOOMCAMP_STORAGE_BUCKET = "dezoomcamp_2024_storage_bucket"
}

variable "project" {
  description = "Your GCP project ID"
  default     = "radiant-gateway-412001"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west1"
  type        = string
}

variable "storage_class" {
  type        = string
  default     = "STANDARD"
  description = "Storage class for your bucket."
}

variable "all_trips_datasets" {
  description = "BigQuery Dataset that raw data will be written to"
  type        = string
  default     = "all_trips_data"
}

variable "homework_trips_datasets" {
  description = "BigQuery Dataset that raw data will be written to"
  type        = string
  default     = "homework_trips_data"
}

variable "nyc_trips_bq_datasets" {
  description = "BigQuery Dataset that raw data will be written to"
  type        = string
  default     = "week4_nyc_trips_data"
}