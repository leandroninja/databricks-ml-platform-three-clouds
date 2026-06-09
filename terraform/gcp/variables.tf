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

variable "gcp_project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "gcp_region" {
  description = "Região GCP principal"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "Nome do bucket GCS do lakehouse (único globalmente)"
  type        = string
}

variable "subnet_cidr" {
  description = "CIDR da subnet principal do Databricks"
  type        = string
  default     = "10.0.0.0/20"
}

variable "pods_cidr" {
  description = "CIDR secundário para pods GKE (usado pelo runtime Databricks no GCP)"
  type        = string
  default     = "10.1.0.0/16"
}

variable "services_cidr" {
  description = "CIDR secundário para services GKE"
  type        = string
  default     = "10.2.0.0/20"
}

variable "gke_master_cidr" {
  description = "CIDR para o master plane do GKE (deve ser /28)"
  type        = string
  default     = "10.3.0.0/28"
}

variable "kms_key_id" {
  description = "ID da chave KMS para encriptação do GCS (deixe vazio para usar Google-managed key)"
  type        = string
  default     = ""
}

variable "databricks_workspace_id" {
  description = "ID do workspace Databricks (para Workload Identity Federation binding)"
  type        = string
  default     = ""
}
