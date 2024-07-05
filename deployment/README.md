# Deployment

This directory contains configuration-as-code to deploy the distributor to supported infrastructure:
 - `modules`: terraform modules to configure infrastructure for running a Tessera log.
   + `gcp`: a Tessera GCP specific terraform module.
 - `live`: example terragrunt configurations for deploying to different environments which use the modules

## Prerequisites

Deploying these examples requires installation of:
 - [`terraform`](https://developer.hashicorp.com/terraform/install) or 
   [`opentofu`](https://opentofu.org/docs/intro/install/)
 - [`terragrunt`](https://terragrunt.gruntwork.io/docs/getting-started/install/)

## Deploying

First authenticate via `gcloud` as a principle with sufficient ACLs for
the project:
```bash
gcloud auth application-default login
```

Terraforming the project can be done by:
 1. `cd` to the relevant `live` directory for the environment to deploy/change
 2. Run `terragrunt apply`

