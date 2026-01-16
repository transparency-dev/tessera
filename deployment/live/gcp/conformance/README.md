# GCP Conformance Configs

This config uses the [gcp/conformance](/deployment/modules/gcp/conformance) module to
define a conformance testing environment. At a high level, this environment consists of:
- Spanner DB,
- GCS Bucket,
- CloudRun service running the [GCP-specific conformance binary](/cmd/conformance/gcp).

The config allows identities (e.g. service accounts) to be provided to allow access to
reading from, and writing to, the log.

## Automatic deployment

For the most part, this terragrunt config is automatically used as part of conformance
testing by the [Cloud Build](/deployment/live/gcp/cloudbuild) pipeline, so doesn't generally
need to be manually applied.

## Manual deployment 

### Prerequisites

You'll need the following tools installed:

- [`golang`](https://go.dev/doc/install)
- [`docker`](https://docs.docker.com/engine/install/)
- [`gcloud`](https://cloud.google.com/sdk/docs/install)
- One of:
   + [`terraform`](https://developer.hashicorp.com/terraform/install) or
   + [`opentofu`](https://opentofu.org/docs/intro/install/)
- [`terragrunt`](https://terragrunt.gruntwork.io/docs/getting-started/install/)

#### Google Cloud tooling

> [!CAUTION]
> This example creates real Google Cloud resources running in your project. They will almost certainly
> cost you real money if left running.  For the purposes of this demo it is strongly recommended that 
> you create a new project so that you can easily clean up at the end.

Once you've got a Google Cloud project you want to use, have configured your local `gcloud`
tool to use it, and authenticated as a principal with sufficient ACLs for the project:

```bash
gcloud config set project {YOUR PROJECT}
gcloud auth application-default login
```

#### Set environment variables

Set the required environment variables:
```bash
# The ID of the Google Cloud Project you're deploying into
export GOOGLE_PROJECT=$(gcloud config get project)

# This should be a note signer string.
# You can use the generate_keys tool to create a new signer & verifier pair:
go run github.com/transparency-dev/serverless-log/cmd/generate_keys@HEAD --key_name="TestTessera" --out_priv=tessera.sec --out_pub=tessera.pub
export TESSERA_SIGNER=$(cat tessera.sec)

# This is the name of the artifact registry docker repo to create/use.
export DOCKER_REPO_NAME=tessera-docker

```

Optionally, set the environment variables below to customize the deployment:
```bash
# GCP region to deploy into (defaults to us-central1)
export GOOGLE_REGION={VALUE} 

# This is used as part of resource names, using this variable will allow you to have multiple deployments in a single project.
export TESSERA_BASE_NAME={VALUE} 

# This allows you to specify the email of an existing service account which should be used by Cloud Run.
# By default, the project's default service account will be used.
export TESSERA_CLOUD_RUN_SERVICE_ACCOUNT={VALUE}

# This allows configuration of which users are allowed to read from the GCS bucket containing the t-log tiles.
# To make the bucket public, set this to "allUsers".
export TESSERA_READER={VALUE}

# This allows configuration of which users are allowed to make HTTP requests to the Cloud Run instance, e.g. to add entries to the t-log.
# By default, only the project's default service account is permitted.
export TESSERA_WRITER={VALUE}
```

#### Set up artifact registry

First, create a new artifact registry based Docker repo:

```bash
gcloud artifacts repositories create ${DOCKER_REPO_NAME} \
        --repository-format=docker \
        --location=us-central1 \
        --description="My Tessera docker repo" \
        --immutable-tags
```

Then authorize your local `docker` command to be able to interact with it:

```bash
gcloud auth configure-docker us-central1-docker.pkg.dev
```

### Process

#### Build & push docker image

You will need to build and push the image created by the
[the /cmd/conformance/gcp/Dockerfile](/cmd/conformance/gcp/Dockerfile) somewhere.
Google Artifact Registry is one option: https://cloud.google.com/build/docs/build-push-docker-image

Note that it's not currently possible to _build_ the docker image with Google Cloud Build (this is because
building from the root directory but referencing Dockerfile in a subdirectory isn't supported), but you can
configure your local `docker` to get access to Artifact Registry using the gcloud CLI credential helper, and
then build locally and push to Artifact Registry. Details on this can be found in the link above.

```bash
docker build . -f ./cmd/conformance/gcp/Dockerfile --tag us-central1-docker.pkg.dev/${GOOGLE_PROJECT}/${DOCKER_REPO_NAME}/conformance:latest

docker push us-central1-docker.pkg.dev/${GOOGLE_PROJECT}/${DOCKER_REPO_NAME}/conformance:latest

# The docker image:tag for the image you just built.
export TESSERA_CLOUD_RUN_DOCKER_IMAGE=us-central1-docker.pkg.dev/${GOOGLE_PROJECT}/${DOCKER_REPO_NAME}/conformance:latest

```

#### Terragrunt apply

Finally, apply the config using `terragrunt`:

 1. `cd` to the relevant directory for the environment to deploy/change (e.g. `ci`)
 2. Run `terragrunt apply`

This should create all necessary infrastructure, and spin up a Cloud Run instance with the
docker image you created above.

### Clean up

> [!IMPORTANT]
> You need to run this step on your project if you want to ensure you don't get charged into perpetuity 
> for the resources we've setup. 

**This will delete your project!**
Do not do this on a project that you didn't create expressly and exclusively to run this demo.
```bash
gcloud projects delete ${GOOGLE_PROJECT}
```
