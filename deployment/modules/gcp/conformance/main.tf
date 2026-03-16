terraform {
  backend "gcs" {}

  required_providers {
    google = {
      source  = "registry.terraform.io/hashicorp/google"
      version = "6.1.0"
    }
  }

  required_version = "= 1.9.8"
}

data "google_compute_default_service_account" "default" {
  depends_on = [
    google_project_service.compute_engine,
  ]
}

locals {
  readers                  = length(var.conformance_readers) > 0 ? var.conformance_readers : ["serviceAccount:${data.google_compute_default_service_account.default.email}"]
  writers                  = length(var.conformance_writers) > 0 ? var.conformance_writers : ["serviceAccount:${data.google_compute_default_service_account.default.email}"]
  cloudrun_service_account = length(var.cloudrun_service_account) > 0 ? var.cloudrun_service_account : data.google_compute_default_service_account.default.email
}


## Call the Tessera GCP module
##
## This will configure all the storage infrastructure required to run a Tessera log on GCP.
module "gcs" {
  source = "..//gcs"

  base_name          = var.base_name
  env                = var.env
  location           = var.location
  project_id         = var.project_id
  bucket_readers     = local.readers
  log_writer_members = ["serviceAccount:${local.cloudrun_service_account}"]
  create_antispam_db = var.enable_antispam
  ephemeral          = true
}

##
## Resources
##

# Enable Cloud Run API
resource "google_project_service" "cloudrun_api" {
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute_engine" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

locals {
  spanner_db_full = "projects/${var.project_id}/instances/${module.gcs.log_spanner_instance.name}/databases/${module.gcs.log_spanner_db.name}"
}

resource "google_cloud_run_v2_service" "default" {
  name         = var.base_name
  location     = var.location
  launch_stage = "GA"

  template {
    service_account                  = local.cloudrun_service_account
    max_instance_request_concurrency = 700
    timeout                          = "5s"

    scaling {
      max_instance_count = 3
    }

    containers {
      image = var.server_docker_image
      name  = "conformance"
      args = [
        "--bucket=${module.gcs.log_bucket.id}",
        "--spanner=${local.spanner_db_full}",
        "--listen=:8080",
        "--signer=${var.signer}",
        "--antispam=${var.enable_antispam}",
      ]
      ports {
        name           = "h2c"
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "2"
          memory = "1024Mi"
        }
      }

      startup_probe {
        initial_delay_seconds = 10
        timeout_seconds       = 10
        period_seconds        = 10
        failure_threshold     = 10
        tcp_socket {
          port = 8080
        }
      }
    }
  }

  deletion_protection = false

  client = "terraform"
  depends_on = [
    module.gcs,
    google_project_service.cloudrun_api,
  ]
}

resource "google_cloud_run_v2_service_iam_binding" "cloudrun_invoker" {
  location = google_cloud_run_v2_service.default.location
  name     = google_cloud_run_v2_service.default.name
  role     = "roles/run.invoker"
  members  = local.writers
}


