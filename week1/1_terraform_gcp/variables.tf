locals {
  data_lake_bucket = "dtc_data_lake"
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