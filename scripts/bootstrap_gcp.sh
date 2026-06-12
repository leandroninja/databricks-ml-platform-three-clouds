#!/usr/bin/env bash
# =============================================================================
# bootstrap_gcp.sh — Setup inicial do ambiente Databricks no GCP
# Leandro Oliveira Moraes
#
# Pré-requisitos:
#   - gcloud CLI instalado e autenticado (gcloud auth login)
#   - Terraform >= 1.6
#   - Databricks CLI >= 0.200
#   - Permissão de Owner ou Editor no projeto GCP
# =============================================================================

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# ---- parâmetros ----
ENVIRONMENT="${ENVIRONMENT:-dev}"
GCP_REGION="${GCP_REGION:-us-central1}"
PROJECT_NAME="${PROJECT_NAME:-databricks-ml-platform}"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-}"    # obrigatório — deve ser passado por env var

if [[ -z "$GCP_PROJECT_ID" ]]; then
  log_error "GCP_PROJECT_ID não definido. Execute: export GCP_PROJECT_ID=meu-projeto-gcp"
  exit 1
fi

GCS_STATE_BUCKET="gcs-terraform-state-leandro"

# ---- validações ----
log_info "Verificando dependências..."

for dep in gcloud terraform databricks jq; do
  if ! command -v "$dep" &>/dev/null; then
    log_error "Dependência não encontrada: $dep"
    exit 1
  fi
  log_info "  $dep OK"
done

# ---- autenticação GCP ----
log_info "Verificando autenticação GCP..."

if ! gcloud auth print-access-token &>/dev/null; then
  log_info "Não autenticado — executando gcloud auth login..."
  gcloud auth login
fi

gcloud config set project "$GCP_PROJECT_ID"
gcloud auth application-default login --quiet    # para o Terraform usar as credenciais

CURRENT_PROJECT=$(gcloud config get-value project)
log_info "Projeto GCP ativo: ${CURRENT_PROJECT}"

# ---- habilita APIs necessárias ----
log_info "Habilitando APIs GCP necessárias..."

APIS=(
  "storage.googleapis.com"
  "compute.googleapis.com"
  "iam.googleapis.com"
  "iamcredentials.googleapis.com"
  "cloudresourcemanager.googleapis.com"
  "serviceusage.googleapis.com"
  "bigquery.googleapis.com"
  "databricks.googleapis.com"
)

for api in "${APIS[@]}"; do
  log_info "  Habilitando: ${api}"
  gcloud services enable "$api" --project="$GCP_PROJECT_ID" --quiet
done

log_info "APIs habilitadas"

# ---- cria bucket GCS para Terraform state ----
log_info "Criando bucket GCS para Terraform state..."

if gsutil ls "gs://${GCS_STATE_BUCKET}" &>/dev/null; then
  log_info "Bucket '${GCS_STATE_BUCKET}' já existe"
else
  gsutil mb \
    -p "$GCP_PROJECT_ID" \
    -c STANDARD \
    -l "$GCP_REGION" \
    "gs://${GCS_STATE_BUCKET}"

  # habilita versioning para proteger o state
  gsutil versioning set on "gs://${GCS_STATE_BUCKET}"

  # bloqueia acesso público
  gsutil pap set enforced "gs://${GCS_STATE_BUCKET}"

  log_info "Bucket GCS de state criado: gs://${GCS_STATE_BUCKET}"
fi

# ---- cria service account para Terraform ----
SA_NAME="sa-terraform-${ENVIRONMENT}"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

log_info "Criando service account para Terraform..."

if gcloud iam service-accounts describe "$SA_EMAIL" --project="$GCP_PROJECT_ID" &>/dev/null; then
  log_info "Service account '${SA_EMAIL}' já existe"
else
  gcloud iam service-accounts create "$SA_NAME" \
    --description="Terraform provisioning — ${ENVIRONMENT}" \
    --display-name="Terraform SA ${ENVIRONMENT}" \
    --project="$GCP_PROJECT_ID"

  # roles necessárias para o Terraform
  ROLES=(
    "roles/editor"
    "roles/iam.serviceAccountAdmin"
    "roles/iam.workloadIdentityPoolAdmin"
    "roles/storage.admin"
    "roles/bigquery.admin"
  )

  for role in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
      --member="serviceAccount:${SA_EMAIL}" \
      --role="$role" \
      --quiet
  done

  log_info "Service account '${SA_EMAIL}' criada e configurada"
fi

# cria e baixa a chave (só usar em dev — em prod usar Workload Identity)
if [[ "$ENVIRONMENT" != "prod" ]]; then
  KEY_FILE="terraform/gcp/sa-key-${ENVIRONMENT}.json"

  if [[ ! -f "$KEY_FILE" ]]; then
    gcloud iam service-accounts keys create "$KEY_FILE" \
      --iam-account="$SA_EMAIL" \
      --project="$GCP_PROJECT_ID"
    log_info "Chave da SA salva em: ${KEY_FILE}"
    log_warn "ATENÇÃO: ${KEY_FILE} contém credenciais — NÃO commitar no git!"
    export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/${KEY_FILE}"
  fi
else
  log_info "Ambiente prod — usando Application Default Credentials (sem chave JSON)"
fi

# ---- gera terraform.tfvars ----
TFVARS_FILE="terraform/gcp/terraform.tfvars.${ENVIRONMENT}"

if [[ ! -f "$TFVARS_FILE" ]]; then
  LAKEHOUSE_BUCKET="lakehouse-${PROJECT_NAME//[_]/-}-${ENVIRONMENT}"

  cat > "$TFVARS_FILE" <<EOF
# gerado por bootstrap_gcp.sh
project_name    = "${PROJECT_NAME}"
environment     = "${ENVIRONMENT}"
gcp_project_id  = "${GCP_PROJECT_ID}"
gcp_region      = "${GCP_REGION}"
gcs_bucket_name = "${LAKEHOUSE_BUCKET}"
subnet_cidr     = "10.0.0.0/20"
pods_cidr       = "10.1.0.0/16"
services_cidr   = "10.2.0.0/20"
gke_master_cidr = "10.3.0.0/28"
EOF

  log_info "Arquivo de variáveis criado: ${TFVARS_FILE}"
else
  log_info "Arquivo ${TFVARS_FILE} já existe"
fi

# ---- próximos passos ----
log_info ""
log_info "=========================================="
log_info "Bootstrap GCP concluído!"
log_info "Próximos passos:"
log_info "  1. cd terraform/gcp"
log_info "  2. terraform init"
log_info "  3. terraform plan -var-file=terraform.tfvars.${ENVIRONMENT}"
log_info "  4. terraform apply -var-file=terraform.tfvars.${ENVIRONMENT}"
log_info "=========================================="
