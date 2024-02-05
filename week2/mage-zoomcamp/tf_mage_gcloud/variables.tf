locals {
  MAGE_DEZOOMCAMP_STORAGE_BUCKET = "mage_dezoomcamp_2024_storage_bucket"
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

variable "nyc_trips_bq_dataset" {
  description = "BigQuery Dataset that raw data will be written to"
  type        = string
  default     = "nyc_trips_dataset"
}