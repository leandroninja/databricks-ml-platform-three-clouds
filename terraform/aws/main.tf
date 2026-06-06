terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.38"
    }
  }

  backend "s3" {
    bucket         = "s3-terraform-state-leandro"
    key            = "databricks-platform/aws.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}

# ---- providers ----

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.tags
  }
}

# provider databricks account-level (para criar workspace E2)
provider "databricks" {
  alias         = "mws"
  host          = "https://accounts.cloud.databricks.com"
  account_id    = var.databricks_account_id
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}

# provider databricks workspace-level (após workspace criado)
provider "databricks" {
  alias = "workspace"
  host  = databricks_mws_workspaces.main.workspace_url
}

# ---- VPC ----

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "vpc-databricks-${var.environment}" }
}

# subnets privadas em 2 AZs (Databricks recomenda pelo menos 2 AZs)
resource "aws_subnet" "private_az1" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, 0)
  availability_zone = "${var.aws_region}a"

  tags = { Name = "snet-databricks-private-az1" }
}

resource "aws_subnet" "private_az2" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, 1)
  availability_zone = "${var.aws_region}b"

  tags = { Name = "snet-databricks-private-az2" }
}

# subnet pública para NAT Gateway
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, 2)
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = { Name = "snet-databricks-public" }
}

# Internet Gateway + NAT Gateway (instâncias privadas precisam de saída)
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "igw-databricks-${var.environment}" }
}

resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "eip-nat-databricks-${var.environment}" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id

  tags = { Name = "nat-databricks-${var.environment}" }

  depends_on = [aws_internet_gateway.main]
}

# route tables
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "rt-public-databricks" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = { Name = "rt-private-databricks" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private_az1" {
  subnet_id      = aws_subnet.private_az1.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_az2" {
  subnet_id      = aws_subnet.private_az2.id
  route_table_id = aws_route_table.private.id
}

# security group para o workspace Databricks
resource "aws_security_group" "databricks" {
  name        = "sg-databricks-${var.environment}"
  description = "Security group para workspace Databricks E2"
  vpc_id      = aws_vpc.main.id

  # permite tráfego interno entre nodes do cluster
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "sg-databricks-${var.environment}" }
}

# ---- S3 Buckets ----

resource "aws_s3_bucket" "lakehouse" {
  bucket        = var.s3_bucket_name
  force_destroy = var.environment != "prod"    # não permite delete acidental em prod

  tags = { Name = var.s3_bucket_name }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"    # usa KMS managed key
    }
    bucket_key_enabled = true    # reduz custo de API KMS
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---- IAM — Cross-account Role para Databricks ----

data "aws_iam_policy_document" "databricks_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::414351767826:root"]    # account ID da Databricks
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }
}

resource "aws_iam_role" "databricks_cross_account" {
  name               = "role-databricks-cross-account-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.databricks_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "databricks_cross_account" {
  name   = "policy-databricks-cross-account"
  role   = aws_iam_role.databricks_cross_account.id
  policy = file("${path.module}/policies/cross_account_policy.json")
}

# ---- Instance Profile para acesso ao S3 nos clusters ----

resource "aws_iam_role" "cluster_instance_profile" {
  name = "role-databricks-cluster-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "cluster_s3" {
  name = "policy-cluster-s3"
  role = aws_iam_role.cluster_instance_profile.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.lakehouse.arn,
          "${aws_s3_bucket.lakehouse.arn}/*"
        ]
      },
      {
        # permite listagem para Auto Loader
        Effect   = "Allow"
        Action   = ["s3:ListAllMyBuckets"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "cluster" {
  name = "ip-databricks-cluster-${var.environment}"
  role = aws_iam_role.cluster_instance_profile.name
}

# ---- Databricks Workspace E2 ----

resource "databricks_mws_networks" "main" {
  provider           = databricks.mws
  account_id         = var.databricks_account_id
  network_name       = "network-${var.environment}"
  security_group_ids = [aws_security_group.databricks.id]
  subnet_ids         = [aws_subnet.private_az1.id, aws_subnet.private_az2.id]
  vpc_id             = aws_vpc.main.id
}

resource "databricks_mws_storage_configurations" "main" {
  provider                   = databricks.mws
  account_id                 = var.databricks_account_id
  storage_configuration_name = "storage-${var.environment}"
  bucket_name                = aws_s3_bucket.lakehouse.bucket
}

resource "databricks_mws_credentials" "main" {
  provider         = databricks.mws
  account_id       = var.databricks_account_id
  credentials_name = "credentials-${var.environment}"
  role_arn         = aws_iam_role.databricks_cross_account.arn
}

resource "databricks_mws_workspaces" "main" {
  provider       = databricks.mws
  account_id     = var.databricks_account_id
  workspace_name = "dbw-${var.project_name}-${var.environment}"
  aws_region     = var.aws_region

  credentials_id           = databricks_mws_credentials.main.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.main.storage_configuration_id
  network_id               = databricks_mws_networks.main.network_id

  token {
    comment = "terraform-provisioning"
  }
}

# ---- locals ----

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Owner       = "leandro.moraes"
  }
}
