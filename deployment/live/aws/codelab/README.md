# AWS codelab deployment

This codelab helps you bring a test Tessera infrastructure on AWS, and
to use it by running a test personality server on an EC2 VM. The infrastructure 
will be comprised of an [Aurora](https://aws.amazon.com/rds/aurora/) MySQL
database and a private [S3](https://aws.amazon.com/s3/) bucket. 

> [!CAUTION]
> 
> This example creates real Amazon Web Services resources running in your
> project. They will cost you real money. For the purposes of this demo 
> it is strongly recommended that you create a new project so that you
> can easily clean up at the end.
 
## Prerequisites
For the remainder of this codelab, you'll need to have an AWS account,
with a running EC2 Amazon Linux VM, and the following software installed:

 - [golang](https://go.dev/doc/install), which we'll use to compile and
   run the test personality on the VM
 - [terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
   and [terragrunt](https://terragrunt.gruntwork.io/docs/getting-started/install/)
   in order to deploy the Tessera infrastructure from the VM.
 - `git` to clone the repo
 - a terminal multiplexer of your choice for convenience

Follow [these
instructions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html)
to set up a VM. A free-tier `t2.micro` VM is enough for this codelab. Leave all
the defaults settings, including for the default VPC. Don't forget to run
`chmod 400` on your SSH key.

## Instructions

 ### Prepare your environment
 1. [SSH to your VM](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html#ec2-connect-to-instance).

 1. Authenticate with a role that has sufficient access to create resources.
    For the purpose of this codelab, and for ease of demonstration, we'll use the
    `AdministratorAccess` role, and authenticate with `aws configure sso`.
    **DO NOT** use this role to run any production infrastructure, or if there are
    *other
    services running on your AWS account.

    Here's an example run:
    ```
    [ec2-user@ip-172-31-21-186 tessera]$ aws configure sso
    SSO session name (Recommended): greenfield-session
    SSO start URL [None]: https://console.aws.amazon.com/ // unless you use a custom signin console
    SSO region [None]: us-east-1
    SSO registration scopes [sso:account:access]:
    Attempting to automatically open the SSO authorization page in your default browser.
    If the browser does not open or you wish to use a different device to authorize this request, open the following URL:
    
    https://device.sso.us-east-1.amazonaws.com/
    
    Then enter the code:
    
    <REDACTED>
    There are 4 AWS accounts available to you.
    Using the account ID <REDACTED>
    The only role available to you is: AdministratorAccess
    Using the role name "AdministratorAccess"
    CLI default client Region [None]: us-east-1
    CLI default output format [None]:
    CLI profile name [AdministratorAccess-<REDACTED>]:
    
    To use this profile, specify the profile name using --profile, as shown:
    
    aws s3 ls --profile AdministratorAccess-<REDACTED>
    ```

 1. Set these environment variables according to the ones you chose when configuring
    your AWS profile:
    ```
    export AWS_REGION=us-east-1
    export AWS_PROFILE=AdministratorAccess-<REDACTED>
    ```

 1. Fetch the Tessera repo, and go to its root:
    ```
    git clone https://github.com/transparency-dev/tessera
    cd tessera/
    ```

### Deploy a Tessera storage infrastructure
In this section, we'll bring up a [S3](https://aws.amazon.com/s3/) bucket, an
[Aurora](https://aws.amazon.com/rds/aurora/) MySQL, and we'll connect them to the
VM.

 1. From the root of the tessera repo, initialize terragrunt:
    ```
    terragrunt init --working-dir=deployment/live/aws/codelab/
    ```

 1. Deploy the infrastructure:
    ```
    terragrunt apply --working-dir=deployment/live/aws/codelab/
    ```
    This brings up the Terraform infrastructure (S3 bucket + DynamoDB table for
    terraform state locking only) and the Tessera infrastructure: an
    RDS Aurora instance, a private S3 bucket, and connects this bucket to the
    default VPC that your VM should be connected to.

 1. Save the RDS instance URI and S3 bucket name for later:
    ```
    export LOG_RDS_DB=$(terragrunt output --working-dir=deployment/live/aws/codelab/ --raw log_rds_db)
    export LOG_BUCKET=$(terragrunt output --working-dir=deployment/live/aws/codelab/ --raw log_bucket_id)
    export LOG_NAME=$(terragrunt output --working-dir=deployment/live/aws/codelab/ --raw log_name)
    ```
 
1. Connect the VM and Aurora database following [these instructions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/tutorial-ec2-rds-option1.html#option1-task3-connect-ec2-instance-to-rds-database),
   it takes a few clicks in the UI.

### Start a Tessera personality
A personality is a server that interacts with Tessera's storage
infrastructure. In this codelab, it accepts POST requests on a `add/` HTTP
endpoint.

 1. Generate the key pair used to sign and verify checkpoints:
    ```
    mkdir -p /home/ec2-user/tessera-keys
    go run github.com/transparency-dev/serverless-log/cmd/generate_keys@80334bc9dc573e8f6c5b3694efad6358da50abd4 \
              --key_name=$LOG_NAME \
              --out_priv=/home/ec2-user/tessera-keys/$LOG_NAME.sec \
              --out_pub=/home/ec2-user/tessera-keys/$LOG_NAME.pub
    ```

 1. Running the commands below will print some easily copy-and-pasteable exports
 which you can use to set up the environment in a second terminal ready to be
 able to send requests:
    ```
    echo "export WRITE_URL=http://localhost:2024/"
    echo "export READ_URL=https://$LOG_BUCKET.s3.$AWS_REGION.amazonaws.com/"
    echo "export LOG_PUBLIC_KEY=$(cat /home/ec2-user/tessera-keys/$LOG_NAME.pub)"
    ```

 1. Run the Conformance personality binary.
    ```
    go run ./cmd/conformance/aws \
      --bucket=$LOG_BUCKET \
      --db_user=root \
      --db_password=password \
      --db_name=tessera \
      --db_host=$LOG_RDS_DB \
      --signer=$(cat /home/ec2-user/tessera-keys/$LOG_NAME.sec) \
      --v=3
    ```

 1. 🎉 **Congratulations** 🎉

    You have successfully brought up Tessera's
    AWS infrastructure, and started a personality server that can add entries to it.

    Use the environment variables from above to interact with the personality in a different terminal.

    This personality accepts `POST` requests to the `/add` endpoint under `WRITE_URL`.
    Log entries can be read directly from S3 without going through the server,
    at `READ_URL`, and checkpoint signatures can be verified with `LOG_PUBLIC_KEY`.

 1. Head over to the [remainder of this codelab](https://github.com/transparency-dev/tessera/tree/main/cmd/conformance#codelab)
    to add leaves to the log and inspect its contents.

> [!IMPORTANT]  
> Do not forget to delete all the resources to avoid incurring any further cost
> when you're done using the log. The easiest way to do this, is to [close the account](https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-closing.html).
> If you prefer to delete the resources with `terragrunt destroy`, bear in mind
> that this command might not destroy all the resources that were created (like
> the S3 bucket or DynamoDB instance Terraform created to store its state for
> instance). If `terragrunt destroy` shows no output, run
> `terragrunt destroy --terragrunt-log-level debug --terragrunt-debug`.

## Trying with Antispam

The instructions above deploy the conformance binary without antispam.
This means that duplicate entries can be written to the log.

For logs that are publicly writable, it may be beneficial to deploy antispam, which is a weak form of deduplication.
The instructions to do this for the codelab are largely the same, except:

 1. When applying the terraform, instruct it to create an additional DB for the antispam tables:
    ```
    terragrunt apply --working-dir=deployment/live/aws/codelab/ -var="create_antispam_db=true"
    ```
 1. When running the conformance binary, pass in two additional flags to configure antispam:
    ```
    go run ./cmd/conformance/aws \
      --bucket=$LOG_BUCKET \
      --db_user=root \
      --db_password=password \
      --db_name=tessera \
      --db_host=$LOG_RDS_DB \
      --signer=$(cat /home/ec2-user/tessera-keys/$LOG_NAME.sec) \
      --v=3 \
      --antispam=true \
      --antispam_db_name=antispam_db
    ```
