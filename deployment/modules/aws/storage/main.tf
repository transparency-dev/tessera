locals {
  name = "${var.prefix_name}-${var.base_name}"
}

terraform {
  required_providers {
    mysql = {
      source  = "petoju/mysql"
      version = "3.0.71"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.region
}

# Resources

## S3 Bucket
resource "aws_s3_bucket" "log_bucket" {
  bucket        = "${local.name}-bucket"
  force_destroy = var.ephemeral
}

## Aurora MySQL RDS database
resource "aws_rds_cluster" "log_rds" {
  apply_immediately  = true
  cluster_identifier = "${local.name}-cluster"
  engine             = "aurora-mysql"
  # TODO(phboneff): make sure that we want to pin this
  engine_version  = "8.0"
  database_name   = "tessera"
  master_username = "root"
  # TODO(phboneff): move to either random strings / Secret Manager / IAM
  master_password         = "password"
  skip_final_snapshot     = true
  backup_retention_period = 1
}

resource "aws_rds_cluster_instance" "cluster_instances" {
  # TODO(phboneff): make some of these variables and/or
  # tweak some of these.
  count              = 1
  identifier         = "${local.name}-writer-${count.index}"
  cluster_identifier = aws_rds_cluster.log_rds.id
  instance_class     = "db.r5.large"
  engine             = aws_rds_cluster.log_rds.engine
  engine_version     = aws_rds_cluster.log_rds.engine_version
}

# Configure the MySQL provider based on the outcome of
# creating the aws_db_instance.
# This requires that the machine running terraform has access
# to the DB instance created above. This is _NOT_ the case when
# github actions are applying the terraform.
provider "mysql" {
  endpoint = aws_rds_cluster_instance.cluster_instances[0].endpoint
  username = aws_rds_cluster.log_rds.master_username
  password = aws_rds_cluster.log_rds.master_password
}

# Create a second database for antispam.
resource "mysql_database" "antispam_db" {
  name  = "antispam_db"
  count = var.create_antispam_db ? 1 : 0
}
