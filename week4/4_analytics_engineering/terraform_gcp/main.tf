terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "data_lake_bucket" {
  name     = "${local.DEZOOMCAMP_STORAGE_BUCKET}_${var.project}"
  location = "US"

  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

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

resource "google_bigquery_dataset" "all_trips_datasets" {
  dataset_id = var.all_trips_datasets
  project    = var.project
  location   = "US"
}

resource "google_bigquery_dataset" "homework_trips_datasets" {
  dataset_id = var.homework_trips_datasets
  project    = var.project
  location   = "US"
}

resource "google_bigquery_dataset" "nyc_trips_bq_datasets" {
  dataset_id = var.nyc_trips_bq_datasets
  project    = var.project
  location   = "US"
}