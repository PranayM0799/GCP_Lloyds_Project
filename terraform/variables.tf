variable "project_id" {
  description = "Your GCP project ID"
  type        = string
  default     = "type_project_id_here"
}

variable "region" {
  description = "The GCP region to deploy resources in"
  type        = string
  default     = "us-central1"
}

variable "dataflow_sa" {
  description = "Service account used to run Dataflow jobs"
  type        = string
  default     = "dataflow-worker@"type_project_id_here".iam.gserviceaccount.com"
}
