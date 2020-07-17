variable "bucket_name" {
    type = string
    description = "Google Cloud Storage Bucket to create"
}

variable "bq_ops_dataset" {
    type = string
    description = "Auxliary bigquery dataset for Megalista operations to create"
}

variable "developer_token" {
    type = string
    description = "Google Ads developer Token"
}

variable "client_id" {
    type = string
    description = "OAuth Client Id"
}

variable "client_secret" {
    type = string
    description = "OAuth Client Secret"
}

variable "access_token" {
    type = string
    description = "Access Token"
}

variable "refresh_token" {
    type = string
    description = "Refresh Token"
}

variable "setup_sheet_id" {
    type = string
    description = "Setup Sheet Id"
}

data "google_client_config" "current" {
}

data "google_client_openid_userinfo" "me" {
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "${var.bq_ops_dataset}"
  location                    = "US"
  delete_contents_on_destroy = true
}

locals {
    scheduler_body = <<EOF
    {
        "jobName": "megalist_daily",
        "parameters": {
            "developer_token": "${var.developer_token}",
            "client_id": "${var.client_id}",
            "client_secret": "${var.client_secret}",
            "access_token": "${var.access_token}",
            "refresh_token": "${var.refresh_token}",
            "setup_sheet_id": "${var.setup_sheet_id}",
            "bq_ops_dataset": "${var.bq_ops_dataset}",
        },
        "environment": {
            "tempLocation": "gs://${var.bucket_name}/tmp",
            "zone": "us-central1-f"
        }
    }
    EOF
}

resource "google_storage_bucket" "my_storage" {
  name          = var.bucket_name
  location      = "US"
  force_destroy = true
  bucket_policy_only = true
}

resource "google_project_iam_member" "cloudscheduler-creator" {
  project = data.google_client_config.current.project
  role    = "roles/cloudscheduler.admin"
  member  = "user:${data.google_client_openid_userinfo.me.email}"
}

resource "google_service_account" "sa" {
  account_id   = "megalista-runner"
  display_name = "Service Account for Megalista use"
}

resource "google_project_iam_member" "dataflow-admin-sa" {
  project = data.google_client_config.current.project
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "project-editor-sa" {
  project = data.google_client_config.current.project
  role    = "roles/editor"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "storage-admin-sa" {
  project = data.google_client_config.current.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "scheduler-sa" {
  project = data.google_client_config.current.project
  role    = "roles/cloudscheduler.admin"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_service" "enable_sheets_api" {
  project = data.google_client_config.current.project
  service = "sheets.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy = false
}

resource "google_project_service" "enable_dataflow_api" {
  project = data.google_client_config.current.project
  service = "dataflow.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy = false
}

resource "google_project_service" "enable_cloudscheduler_api" {
  project = data.google_client_config.current.project
  service = "cloudscheduler.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy = false
}

resource "google_project_service" "enable_analytics_api" {
  project = data.google_client_config.current.project
  service = "analytics.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy = false
}

resource "google_project_service" "enable_ads_api" {
  project = data.google_client_config.current.project
  service = "googleads.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy = false
}

resource "google_project_service" "enable_dcm_api" {
  project = data.google_client_config.current.project
  service = "dfareporting.googleapis.com"
  disable_dependent_services = true
  disable_on_destroy = false
}

resource "google_cloud_scheduler_job" "megalista_job" {
  depends_on       = [google_project_service.enable_cloudscheduler_api]
  name             = "megalista_job"
  description      = "Daily Runner for Megalista"
  schedule         = "0 0 * * *"
  time_zone        = "America/Sao_Paulo"
  attempt_deadline = "320s"
  region           = "us-central1"

  http_target {
    http_method = "POST"
    uri         = "https://dataflow.googleapis.com/v1b3/projects/${data.google_client_config.current.project}/templates:launch?gcsPath=gs://${var.bucket_name}/templates/megalist"
    body        = base64encode(local.scheduler_body)
    oauth_token {
      service_account_email = google_service_account.sa.email
      scope = "https://www.googleapis.com/auth/compute"
    }
  }
}
