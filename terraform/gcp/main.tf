terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.15"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.38"
    }
  }

  backend "gcs" {
    bucket = "gcs-terraform-state-leandro"
    prefix = "databricks-platform/gcp"
  }
}

# ---- providers ----

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

provider "databricks" {
  alias = "workspace"
  host  = "https://${google_databricks_workspace.main.workspace_url}"
}

# ---- GCS Bucket do Lakehouse ----

resource "google_storage_bucket" "lakehouse" {
  name          = var.gcs_bucket_name
  project       = var.gcp_project_id
  location      = var.gcp_region
  storage_class = "STANDARD"
  force_destroy = var.environment != "prod"

  # versioning habilitado — necessário para Auto Loader com GCS
  versioning {
    enabled = true
  }

  # lifecycle rule: move objetos +30 dias para Nearline, +90 para Coldline
  lifecycle_rule {
    condition { age = 30 }
    action { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }

  lifecycle_rule {
    condition { age = 90 }
    action { type = "SetStorageClass"; storage_class = "COLDLINE" }
  }

  # encriptação com CMEK (Customer Managed Encryption Key)
  dynamic "encryption" {
    for_each = var.kms_key_id != "" ? [1] : []
    content {
      default_kms_key_name = var.kms_key_id
    }
  }

  uniform_bucket_level_access = true    # sem ACLs por objeto — usa IAM

  labels = local.labels
}

# pastas lógicas (prefixos) no GCS
resource "google_storage_bucket_object" "prefixes" {
  for_each = toset(["raw/", "bronze/", "silver/", "gold/", "checkpoints/", "quarantine/"])

  bucket  = google_storage_bucket.lakehouse.name
  name    = each.key
  content = " "    # objeto placeholder para criar o prefixo
}

# ---- VPC Network ----

resource "google_compute_network" "databricks" {
  name                    = "vpc-databricks-${var.environment}"
  project                 = var.gcp_project_id
  auto_create_subnetworks = false
  mtu                     = 1460
}

resource "google_compute_subnetwork" "databricks" {
  name          = "snet-databricks-${var.environment}"
  project       = var.gcp_project_id
  region        = var.gcp_region
  network       = google_compute_network.databricks.id
  ip_cidr_range = var.subnet_cidr

  # secondary ranges obrigatórios para GKE-based Databricks no GCP
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }

  private_ip_google_access = true    # permite acesso às APIs do Google sem IP público
}

# firewall rules para Databricks
resource "google_compute_firewall" "databricks_internal" {
  name    = "fw-databricks-internal-${var.environment}"
  network = google_compute_network.databricks.name
  project = var.gcp_project_id

  # permite tráfego interno entre nodes do cluster
  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }

  source_ranges = [var.subnet_cidr]
  target_tags   = ["databricks-node"]
}

# ---- Service Account para Databricks ----

resource "google_service_account" "databricks" {
  account_id   = "sa-databricks-${var.environment}"
  display_name = "Databricks Workspace SA — ${var.environment}"
  project      = var.gcp_project_id
}

# permissão no bucket GCS
resource "google_storage_bucket_iam_member" "databricks_gcs" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.databricks.email}"
}

# BigQuery — leitura (para ingestão BigQuery → Delta)
resource "google_project_iam_member" "databricks_bq_viewer" {
  project = var.gcp_project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.databricks.email}"
}

resource "google_project_iam_member" "databricks_bq_job" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.databricks.email}"
}

# ---- Workload Identity Federation ----
# Permite que o workspace Databricks assuma a SA sem arquivo de chave JSON.
# O Databricks autentica via token OIDC → troca pelo token da SA no GCP.

resource "google_iam_workload_identity_pool" "databricks" {
  project                   = var.gcp_project_id
  workload_identity_pool_id = "pool-databricks-${var.environment}"
  display_name              = "Databricks Workload Identity Pool"

  lifecycle {
    prevent_destroy = true    # pools não podem ser recriados facilmente
  }
}

resource "google_iam_workload_identity_pool_provider" "databricks" {
  project                            = var.gcp_project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.databricks.workload_identity_pool_id
  workload_identity_pool_provider_id = "provider-databricks"

  oidc {
    issuer_uri = "https://accounts.cloud.databricks.com"
  }

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.workspace"  = "assertion.workspace_id"
  }
}

resource "google_service_account_iam_member" "workload_identity" {
  service_account_id = google_service_account.databricks.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.databricks.name}/attribute.workspace/${var.databricks_workspace_id}"
}

# ---- Databricks Workspace GCP ----

# nota: o recurso google_databricks_workspace ainda não existe no provider google oficial
# na prática, o workspace GCP é criado via API do Databricks ou UI, e gerenciado como
# data source aqui. Este bloco é um exemplo de como seria quando o recurso existir.
# Por enquanto, usamos um null_resource com local-exec para criar via CLI Databricks.

resource "null_resource" "databricks_workspace_gcp" {
  triggers = {
    project_id  = var.gcp_project_id
    environment = var.environment
    network     = google_compute_network.databricks.name
    subnet      = google_compute_subnetwork.databricks.name
  }

  provisioner "local-exec" {
    command = <<-EOT
      databricks account workspaces create \
        --workspace-name "dbw-${var.project_name}-${var.environment}" \
        --cloud gcp \
        --gcp-managed-network-config-gke-cluster-master-ip-range ${var.gke_master_cidr} \
        --gcp-network-config-network-project-id ${var.gcp_project_id} \
        --gcp-network-config-vpc-id ${google_compute_network.databricks.name} \
        --gcp-network-config-subnet-id ${google_compute_subnetwork.databricks.name} \
        --gcp-network-config-subnet-region ${var.gcp_region}
    EOT
  }
}

# ---- locals ----

locals {
  labels = {
    project     = replace(var.project_name, "-", "_")
    environment = var.environment
    managed_by  = "terraform"
    owner       = "leandro_moraes"
  }
}
