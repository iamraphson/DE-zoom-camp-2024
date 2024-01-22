terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = var.project
  region = var.region
}

//ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data_lake_bucket" {
  name          = "${local.data_lake_bucket}_${var.project}"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}