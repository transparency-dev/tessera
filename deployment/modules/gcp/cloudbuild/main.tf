terraform {
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_artifact_registry_repository" "docker" {
  repository_id = "docker-${var.env}"
  location      = var.region
  description   = "Tessera conformance docker images"
  format        = "DOCKER"
}

locals {
  artifact_repo                = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker.name}"
  conformance_gcp_docker_image = "${local.artifact_repo}/conformance-gcp"
}

resource "google_cloudbuild_trigger" "docker" {
  name            = "GCP-integration-test-${var.env}"
  service_account = "projects/${var.project_id}/serviceAccounts/${var.service_account}"
  location        = var.region

  github {
    owner = "transparency-dev"
    name  = "tessera"
    push {
      branch = "^main$"
    }
  }

  build {
    ## Destroy any pre-existing deployment/live/gcp/conformance/ci environment.
    ## This might happen if a previous cloud build failed for some reason.
    step {
      id     = "preclean_env"
      name   = "alpine/terragrunt:1.9.8"
      script = <<EOT
        terragrunt --terragrunt-non-interactive --terragrunt-no-color destroy -auto-approve -no-color 2>&1
      EOT
      dir    = "deployment/live/gcp/conformance/ci"
      env = [
        "TESSERA_SIGNER=unused",
        "TESSERA_CLOUD_RUN_DOCKER_IMAGE=${local.conformance_gcp_docker_image}:latest",
        "TESSERA_CLOUD_RUN_SERVICE_ACCOUNT=cloudrun-ci-sa@trillian-tessera.iam.gserviceaccount.com",
        "TESSERA_READER=serviceAccount:cloudbuild-prod-sa@trillian-tessera.iam.gserviceaccount.com",
        "TESSERA_WRITER=serviceAccount:cloudbuild-prod-sa@trillian-tessera.iam.gserviceaccount.com",
        "GOOGLE_PROJECT=${var.project_id}",
        "TF_IN_AUTOMATION=1",
        "TF_INPUT=false",
        "TF_VAR_project_id=${var.project_id}"
      ]
    }
    ## Build the GCP conformance server docker image.
    ## This will be used by the conformance terragrunt config step further down.
    step {
      id   = "docker_build_conformance_gcp"
      name = "gcr.io/cloud-builders/docker"
      args = [
        "build",
        "-t", "${local.conformance_gcp_docker_image}:$SHORT_SHA",
        "-t", "${local.conformance_gcp_docker_image}:latest",
        "-f", "./cmd/conformance/gcp/Dockerfile",
        "."
      ]
    }
    ## Push the image.
    step {
      id   = "docker_push_conformance_gcp"
      name = "gcr.io/cloud-builders/docker"
      args = [
        "push",
        "--all-tags",
        local.conformance_gcp_docker_image
      ]
      wait_for = ["docker_build_conformance_gcp"]
    }
    step {
      id     = "generate_keys"
      name   = "golang"
      script = <<EOT
        go run github.com/transparency-dev/serverless-log/cmd/generate_keys@80334bc9dc573e8f6c5b3694efad6358da50abd4 \
          --key_name=tessera/test/conformance \
          --out_priv=/workspace/key.sec \
          --out_pub=/workspace/key.pub
        cat /workspace/key.pub
      EOT
    }
    ## Apply the deployment/live/gcp/conformance/ci terragrunt config.
    ## This will bring up the conformance infrastructure, including a service
    ## running the conformance server docker image built above.
    step {
      id     = "terraform_apply_conformance_ci"
      name   = "alpine/terragrunt:1.9.8"
      script = <<EOT
        export TESSERA_SIGNER=$(cat /workspace/key.sec)
        terragrunt --terragrunt-non-interactive --terragrunt-no-color apply -auto-approve -no-color 2>&1
      EOT
      dir    = "deployment/live/gcp/conformance/ci"
      env = [
        "GOOGLE_PROJECT=${var.project_id}",
        "TESSERA_CLOUD_RUN_DOCKER_IMAGE=${local.conformance_gcp_docker_image}:latest",
        "TESSERA_CLOUD_RUN_SERVICE_ACCOUNT=cloudrun-ci-sa@trillian-tessera.iam.gserviceaccount.com",
        "TESSERA_READER=serviceAccount:cloudbuild-prod-sa@trillian-tessera.iam.gserviceaccount.com",
        "TESSERA_WRITER=serviceAccount:cloudbuild-prod-sa@trillian-tessera.iam.gserviceaccount.com",
        "TF_IN_AUTOMATION=1",
        "TF_INPUT=false",
        "TF_VAR_project_id=${var.project_id}"
      ]
      wait_for = ["docker_push_conformance_gcp", "generate_keys"]
    }
    ## Grab some outputs from the terragrunt apply above (e.g. conformance server URL) and store
    ## them in files under /workspace. These are needed for later steps.
    step {
      id       = "terraform_outputs"
      name   = "alpine/terragrunt:1.9.8"
      script   = <<EOT
        cd deployment/live/gcp/conformance/ci
        export GOOGLE_PROJECT=${var.project_id}
        export TESSERA_SIGNER=$(cat /workspace/key.sec)
        export TESSERA_CLOUD_RUN_DOCKER_IMAGE=unused,
        terragrunt output --raw conformance_url > /workspace/conformance_url
        terragrunt output --raw conformance_bucket_name > /workspace/conformance_bucket_name
      EOT
      wait_for = ["terraform_apply_conformance_ci"]
    }
    ## Since the conformance infrastructure is not publicly accessible, we need to use bearer tokens
    ## for the hammer to access them.
    ## This step creates those, and stores them for later use.
    step {
      id       = "access"
      name     = "gcr.io/cloud-builders/gcloud"
      script   = <<EOT
      gcloud auth print-access-token > /workspace/cb_access
      curl -H "Metadata-Flavor: Google" "http://metadata/computeMetadata/v1/instance/service-accounts/${var.service_account}/identity?audience=$(cat /workspace/conformance_url)" > /workspace/cb_identity
      EOT
      wait_for = ["terraform_outputs"]
    }
    ## Run the hammer against the conformance server.
    ## We're using it in "target throughput" mode.
    step {
      id       = "hammer"
      name     = "golang"
      script   = <<EOT
        apt update && apt install -y retry
        retry -t 5 -d 15 --until=success go run ./internal/hammer \
            --log_public_key=$(cat /workspace/key.pub) \
            --log_url=https://storage.googleapis.com/$(cat /workspace/conformance_bucket_name)/ \
            --write_log_url="$(cat /workspace/conformance_url)" \
            -v=1 \
            --show_ui=false \
            --bearer_token="$(cat /workspace/cb_access)" \
            --bearer_token_write="$(cat /workspace/cb_identity)" \
            --logtostderr \
            --num_writers=1100 \
            --max_write_ops=1500 \
            --leaf_min_size=1024 \
            --leaf_write_goal=50000
      EOT
      wait_for = ["terraform_outputs", "generate_keys", "access"]
    }

    options {
      logging      = "CLOUD_LOGGING_ONLY"
      machine_type = "E2_HIGHCPU_8"
    }
  }
}

