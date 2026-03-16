# Header ######################################################################
terraform {
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.76.0"
    }
  }
}

locals {
  name = "${var.prefix_name}-${var.base_name}"
  port = 2024
}

provider "aws" {
  region = var.region
}

module "storage" {
  source = "../storage"

  prefix_name        = var.prefix_name
  base_name          = var.base_name
  region             = var.region
  ephemeral          = true
  create_antispam_db = var.create_antispam_db
}

# Resources ####################################################################
## ECS cluster #################################################################
# This will be used to run the conformance and hammer binaries on Fargate.
resource "aws_ecs_cluster" "ecs_cluster" {
  name = local.name
}

resource "aws_ecs_cluster_capacity_providers" "ecs_capacity" {
  cluster_name = aws_ecs_cluster.ecs_cluster.name

  capacity_providers = ["FARGATE"]
}

## Virtual private network #####################################################
# This will be used for the containers to communicate between themselves, and
# the S3 bucket.
resource "aws_default_vpc" "default" {
  tags = {
    Name = "Default VPC"
  }
}

data "aws_subnets" "subnets" {
  filter {
    name   = "vpc-id"
    values = [aws_default_vpc.default.id]
  }
}

## Service discovery ###########################################################
# This will by the hammer to contact multiple conformance tasks with a single
# dns name.
resource "aws_service_discovery_private_dns_namespace" "internal" {
  name = "internal"
  vpc  = aws_default_vpc.default.id
}

resource "aws_service_discovery_service" "conformance_discovery" {
  name = "conformance-discovery"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.internal.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    // TODO(phboneff): make sure that the hammer uses multiple IPs
    // otherwise, set a low TTL and use WEIGHTED.
    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

## Connect S3 bucket to VPC ####################################################
# This allows the hammer to talk to a non public S3 bucket over HTTP.
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_default_vpc.default.id
  service_name = "com.amazonaws.${var.region}.s3"
}


resource "aws_vpc_endpoint_route_table_association" "private_s3" {
  vpc_endpoint_id = aws_vpc_endpoint.s3.id
  route_table_id  = aws_default_vpc.default.default_route_table_id
}

resource "aws_s3_bucket_policy" "allow_access_from_vpce" {
  bucket = module.storage.log_bucket.id
  policy = data.aws_iam_policy_document.allow_access_from_vpce.json
}

data "aws_iam_policy_document" "allow_access_from_vpce" {
  statement {
    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = [
      "s3:GetObject",
    ]

    resources = [
      "${module.storage.log_bucket.arn}/*",
    ]

    condition {
      test     = "StringEquals"
      variable = "aws:sourceVpce"
      values   = [aws_vpc_endpoint.s3.id]
    }
  }
  depends_on = [aws_vpc_endpoint.s3]
}

## Conformance task and service ################################################
# This will start multiple conformance tasks on Fargate within a service.
resource "aws_ecs_task_definition" "conformance" {
  family                   = "conformance"
  requires_compatibilities = ["FARGATE"]
  # Required network_mode for tasks running on Fargate.
  network_mode       = "awsvpc"
  cpu                = 1024
  memory             = 2048
  execution_role_arn = var.ecs_execution_role
  # We need a special role that has access to S3.
  task_role_arn = var.ecs_conformance_task_role
  container_definitions = jsonencode([{
    "name" : "${local.name}-conformance",
    "image" : "${var.ecr_registry}/${var.ecr_repository_conformance}",
    "cpu" : 0,
    "portMappings" : [{
      "name" : "conformance-${local.port}-tcp",
      "containerPort" : local.port,
      "hostPort" : local.port,
      "protocol" : "tcp",
      "appProtocol" : "http"
    }],
    "essential" : true,
    "command" : [
      "--signer=${var.signer}",
      "--bucket=${module.storage.log_bucket.id}",
      "--db_user=root",
      "--db_password=password",
      "--db_name=tessera",
      "--db_host=${module.storage.log_rds_db.endpoint}",
      "--antispam=${var.antispam}",
      "--antispam_db_name=${var.antispam_db_name}",
      "--slog_level=-4"
    ],
    "logConfiguration" : {
      "logDriver" : "awslogs",
      "options" : {
        "awslogs-group" : "/ecs/${local.name}",
        "mode" : "non-blocking",
        "awslogs-create-group" : "true",
        "max-buffer-size" : "25m",
        "awslogs-region" : "us-east-1",
        "awslogs-stream-prefix" : "ecs"
      },
    },
    }, {
    "name" : "aws-otel-collector-${local.name}",
    "image" : "public.ecr.aws/aws-observability/aws-otel-collector:v0.43.1",
    "command" : ["--config=/etc/ecs/ecs-cloudwatch-xray.yaml"],
    "essential" : true,
    "logConfiguration" : {
      "logDriver" : "awslogs",
      "options" : {
        "awslogs-group" : "/ecs/${local.name}",
        "awslogs-region" : "us-east-1",
        "awslogs-stream-prefix" : "ecs",
        "awslogs-create-group" : "true"
      }
    },
    "healthCheck" : {
      "command" : ["/healthcheck"],
      "interval" : 5,
      "timeout" : 6,
      "retries" : 5,
      "startPeriod" : 1
    },
    "portMappings" : [],
  }])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  depends_on = [module.storage]
}

resource "aws_ecs_service" "conformance_service" {
  name                  = local.name
  task_definition       = aws_ecs_task_definition.conformance.arn
  cluster               = aws_ecs_cluster.ecs_cluster.arn
  launch_type           = "FARGATE"
  desired_count         = 3
  wait_for_steady_state = true

  network_configuration {
    subnets = data.aws_subnets.subnets.ids
    # required to access container registry
    assign_public_ip = true
  }

  # connect the service with the service discovery defined above
  service_registries {
    registry_arn = aws_service_discovery_service.conformance_discovery.arn
  }

  depends_on = [
    aws_service_discovery_private_dns_namespace.internal,
    aws_service_discovery_service.conformance_discovery,
    aws_ecs_cluster.ecs_cluster,
    aws_ecs_task_definition.conformance,
  ]
}

## Hammer task definition and execution ########################################
# The hammer can also be launched manually with the following command: 
# aws ecs run-task \
#   --cluster="$(terragrunt output -raw ecs_cluster)" \
#   --task-definition=hammer \
#   --count=1 \
#   --launch-type=FARGATE \
#   --network-configuration='{"awsvpcConfiguration": {"assignPublicIp":"ENABLED","subnets": '$(terragrunt output -json vpc_subnets)'}}'

resource "aws_ecs_task_definition" "hammer" {
  family                   = "hammer"
  requires_compatibilities = ["FARGATE"]
  # Required network_mode for tasks running on Fargate
  network_mode       = "awsvpc"
  cpu                = 1024
  memory             = 2048
  execution_role_arn = var.ecs_execution_role
  container_definitions = jsonencode([{
    "name" : "${local.name}-hammer",
    "image" : "${var.ecr_registry}/${var.ecr_repository_hammer}",
    "cpu" : 0,
    "portMappings" : [{
      "name" : "hammer-80-tcp",
      "containerPort" : 80,
      "hostPort" : 80,
      "protocol" : "tcp",
      "appProtocol" : "http"
    }],
    "essential" : true,
    "command" : [
      "--log_public_key=${var.verifier}",
      "--log_url=https://${module.storage.log_bucket.bucket_regional_domain_name}",
      "--write_log_url=http://${aws_service_discovery_service.conformance_discovery.name}.${aws_service_discovery_private_dns_namespace.internal.name}:${local.port}",
      "--slog_level=-4",
      "--show_ui=false",
      "--num_writers=1100",
      "--max_write_ops=1500",
      "--leaf_min_size=1024",
      "--leaf_write_goal=50000"
    ],
    "logConfiguration" : {
      "logDriver" : "awslogs",
      "options" : {
        "awslogs-group" : "/ecs/${local.name}-hammer",
        "mode" : "non-blocking",
        "awslogs-create-group" : "true",
        "max-buffer-size" : "25m",
        "awslogs-region" : "us-east-1",
        "awslogs-stream-prefix" : "ecs"
      },
    },
  }])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  depends_on = [
    module.storage,
    aws_ecs_cluster.ecs_cluster,
  ]
}


