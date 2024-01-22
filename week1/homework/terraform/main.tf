terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

//ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data_lake_bucket" {
  name     = "${local.DEZOOMCAMP_STORAGE_BUCKET}_${var.project}"
  location = var.region

  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 80 //days
    }
  }

  force_destroy = true
}


//ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.nyc_trips_bq_dataset
  project    = var.project
  location   = var.region
}
