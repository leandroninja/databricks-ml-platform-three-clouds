variable "project_name" {
  description = "Nome do projeto — usado como prefixo nos recursos"
  type        = string
  default     = "databricks-ml-platform"
}

variable "environment" {
  description = "Ambiente de deployment (dev, staging, prod)"
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment deve ser dev, staging ou prod"
  }
}

variable "location" {
  description = "Região Azure principal"
  type        = string
  default     = "brazilsouth"
}

variable "resource_group_name" {
  description = "Nome do resource group principal"
  type        = string
}

variable "storage_account_name" {
  description = "Nome do storage account ADLS Gen2 (3-24 chars, lowercase, sem hífens)"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "storage_account_name deve ter 3-24 chars alfanuméricos minúsculos"
  }
}

variable "metastore_admin_group" {
  description = "Nome do grupo AAD que será admin do Unity Catalog Metastore"
  type        = string
  default     = "databricks-platform-admins"
}

variable "allowed_ip_ranges" {
  description = "Lista de IPs/CIDRs com acesso ao workspace (para allowlist de rede)"
  type        = list(string)
  default     = []
}

variable "databricks_admins" {
  description = "Lista de emails dos admins do workspace Databricks"
  type        = list(string)
  default     = []
}

variable "tags_extras" {
  description = "Tags adicionais para aplicar nos recursos (merge com tags padrão)"
  type        = map(string)
  default     = {}
}
