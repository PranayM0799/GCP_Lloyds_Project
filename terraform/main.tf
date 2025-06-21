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

resource "google_project_iam_member" "dataflow_worker" {
  role   = "roles/dataflow.worker"
  member = "serviceAccount:${var.dataflow_sa}"
}
