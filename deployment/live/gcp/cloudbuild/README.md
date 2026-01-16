# Cloudbuild Triggers and Steps

This directory contains terragrunt files to configure our CloudBuild pipeline(s).
See links in the [deployment dir](/deployment/README.md) to install necessary tools.

The CloudBuild pipeline is triggered on commits to the `main` branch of the repo, and
is responsible for:
 1. Building the `cmd/gcp/conformance` docker image from the `main` branch,
 2. Creating a fresh conformance testing environment,
 3. Running the conformance test against the newly built conformance docker image,
 4. Turning-down the conformance testing environment.

## Initial setup

The first time this is run for a pair of {GCP Project, GitHub Repo} you will get an error 
message such as the following:

```
Error: Error creating Trigger: googleapi: Error 400: Repository mapping does not exist. Please visit $URL to connect a repository to your project
```

This is a manual one-time step that needs to be followed to integrate GCB and the GitHub
project.
