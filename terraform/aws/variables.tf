variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "databricks-ml-platform"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment deve ser dev, staging ou prod"
  }
}

variable "aws_region" {
  description = "Região AWS"
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block da VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "s3_bucket_name" {
  description = "Nome do bucket S3 do lakehouse (deve ser único globalmente)"
  type        = string
}

variable "databricks_account_id" {
  description = "ID da conta Databricks (E2)"
  type        = string
  sensitive   = true
}

variable "databricks_client_id" {
  description = "Client ID do service principal Databricks (para Terraform)"
  type        = string
  sensitive   = true
}

variable "databricks_client_secret" {
  description = "Client secret do service principal Databricks"
  type        = string
  sensitive   = true
}

variable "allowed_ips_cidr" {
  description = "CIDRs com acesso liberado ao workspace (via IP allowlist)"
  type        = list(string)
  default     = []
}
