provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_lake" {
  name     = "${var.project_id}-data-lake"
  location = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "customer_analytics"
  location   = var.region
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-worker"
  display_name = "Dataflow Worker"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

