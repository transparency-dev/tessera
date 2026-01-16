# AWS Conformance Configs

This config uses the [aws/conformance](/deployment/modules/aws/conformance)
module to define a conformance testing environment, actuated by the [AWS
Conformance
Test](/.github/workflows/aws_integration_test.yml)
GitHub action. At a high level, this environment consists of:
 - Aurora MySQL database
 - S3 Bucket
 - ECS+Fargate service running the AWS-specific conformance binary and hammer

## Prerequisites

You'll need to have configured the right IAM permissions to create S3 buckets
and RDS databases, and configured a local AWS profile that can make use of
these permissions.

TODO(phboneff): establish what's the minimum set of permissions we need, and list
them here.

## Manual deployment

Configure an AWS profile on your workstation using your preferred method, (e.g
[sso](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html)
or [credential
files](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html))

Set the required environment variables:
```bash
export AWS_PROFILE={VALUE}
```

Optionally, customize the AWS region (defaults to "us-east-1"), prefix, and base
name for resources (defaults to "tessera" and "conformance"):
```bash
export TESSERA_BASE_NAME={VALUE}
export TESSERA_PREFIX_NAME={VALUE}
```

Resources will be named using a `${TESSERA_PREFIX_NAME}-${TESSERA_BASE_NAME}`
convention.

Terraforming the project can be done by:
 1. `cd` to the relevant directory for the environment to deploy/change (e.g. `ci`)
 2. Run `terragrunt apply`
