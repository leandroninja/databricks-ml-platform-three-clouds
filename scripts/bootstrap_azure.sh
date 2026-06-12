#!/usr/bin/env bash
# =============================================================================
# bootstrap_azure.sh — Setup inicial do ambiente Databricks no Azure
# Leandro Oliveira Moraes — leandro.moraes@empresa.com
#
# O que esse script faz:
#   1. Valida dependências (az CLI, terraform, databricks CLI)
#   2. Autentica no Azure
#   3. Cria resource group e storage account para Terraform state
#   4. Inicializa variáveis de ambiente para o Terraform
#   5. Instala extensão do Databricks no az CLI se necessário
#
# Pré-requisitos:
#   - az CLI >= 2.55
#   - Terraform >= 1.6
#   - Databricks CLI >= 0.200
#   - jq instalado (para parsing JSON)
#   - Permissão de Contributor na subscription Azure
# =============================================================================

set -euo pipefail

# ---- cores para output ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'    # no color

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# ---- parâmetros (podem ser sobrescritos por variáveis de ambiente) ----
ENVIRONMENT="${ENVIRONMENT:-dev}"
LOCATION="${LOCATION:-brazilsouth}"
PROJECT_NAME="${PROJECT_NAME:-databricks-ml-platform}"

RESOURCE_GROUP_STATE="rg-terraform-state"
STORAGE_ACCOUNT_STATE="stterraformstateleandro"
CONTAINER_STATE="tfstate"

# ---- validação de dependências ----
log_info "Verificando dependências..."

check_dep() {
  if ! command -v "$1" &>/dev/null; then
    log_error "Dependência não encontrada: $1. Instale antes de continuar."
    exit 1
  fi
  log_info "  $1: $(${1} --version 2>&1 | head -1)"
}

check_dep az
check_dep terraform
check_dep databricks
check_dep jq

# ---- autenticação Azure ----
log_info "Verificando autenticação Azure..."

if ! az account show &>/dev/null; then
  log_info "Não autenticado — executando az login..."
  az login
fi

SUBSCRIPTION_ID=$(az account show --query id -o tsv)
TENANT_ID=$(az account show --query tenantId -o tsv)
log_info "Subscription: ${SUBSCRIPTION_ID} | Tenant: ${TENANT_ID}"

# confirma subscription correta
read -r -p "$(echo -e "${YELLOW}Continuar nessa subscription? [s/N]: ${NC}")" confirm
if [[ "$confirm" != "s" && "$confirm" != "S" ]]; then
  log_warn "Abortado pelo usuário."
  az account list --output table
  exit 0
fi

# ---- cria resource group e storage para Terraform state ----
log_info "Criando infrastructure para Terraform state..."

if ! az group show --name "$RESOURCE_GROUP_STATE" &>/dev/null; then
  az group create \
    --name     "$RESOURCE_GROUP_STATE" \
    --location "$LOCATION" \
    --tags     "project=$PROJECT_NAME" "managed_by=bootstrap_script"
  log_info "Resource group '$RESOURCE_GROUP_STATE' criado"
else
  log_info "Resource group '$RESOURCE_GROUP_STATE' já existe — pulando criação"
fi

if ! az storage account show --name "$STORAGE_ACCOUNT_STATE" --resource-group "$RESOURCE_GROUP_STATE" &>/dev/null; then
  az storage account create \
    --name                "$STORAGE_ACCOUNT_STATE" \
    --resource-group      "$RESOURCE_GROUP_STATE" \
    --location            "$LOCATION" \
    --sku                 "Standard_LRS" \
    --kind                "StorageV2" \
    --min-tls-version     "TLS1_2" \
    --allow-blob-public-access false
  log_info "Storage account '$STORAGE_ACCOUNT_STATE' criado"
else
  log_info "Storage account '$STORAGE_ACCOUNT_STATE' já existe — pulando"
fi

az storage container create \
  --name                "$CONTAINER_STATE" \
  --account-name        "$STORAGE_ACCOUNT_STATE" \
  --auth-mode           login \
  --public-access       off 2>/dev/null || true

log_info "Container '$CONTAINER_STATE' configurado"

# ---- cria service principal para Terraform ----
log_info "Criando service principal para Terraform..."

SP_NAME="sp-terraform-databricks-${ENVIRONMENT}"

if az ad sp show --id "http://${SP_NAME}" &>/dev/null; then
  log_warn "Service principal '${SP_NAME}' já existe — pulando criação"
else
  SP_JSON=$(az ad sp create-for-rbac \
    --name        "$SP_NAME" \
    --role        "Contributor" \
    --scopes      "/subscriptions/${SUBSCRIPTION_ID}" \
    --output      json)

  SP_CLIENT_ID=$(echo "$SP_JSON" | jq -r '.appId')
  SP_CLIENT_SECRET=$(echo "$SP_JSON" | jq -r '.password')

  log_info "Service principal criado: ${SP_CLIENT_ID}"

  # salva em arquivo local (NUNCA commitar)
  ENV_FILE="terraform/azure/.env.${ENVIRONMENT}"
  cat > "$ENV_FILE" <<EOF
# gerado por bootstrap_azure.sh — NÃO commitar no git
export ARM_SUBSCRIPTION_ID="${SUBSCRIPTION_ID}"
export ARM_TENANT_ID="${TENANT_ID}"
export ARM_CLIENT_ID="${SP_CLIENT_ID}"
export ARM_CLIENT_SECRET="${SP_CLIENT_SECRET}"
EOF

  log_info "Credenciais salvas em: ${ENV_FILE}"
  log_warn "ATENÇÃO: ${ENV_FILE} contém credenciais — não commitar!"
fi

# ---- gera arquivo terraform.tfvars ----
TFVARS_FILE="terraform/azure/terraform.tfvars.${ENVIRONMENT}"

if [[ ! -f "$TFVARS_FILE" ]]; then
  cat > "$TFVARS_FILE" <<EOF
# gerado por bootstrap_azure.sh — ajuste conforme necessário
project_name          = "${PROJECT_NAME}"
environment           = "${ENVIRONMENT}"
location              = "${LOCATION}"
resource_group_name   = "rg-${PROJECT_NAME}-${ENVIRONMENT}"
storage_account_name  = "stleandro${ENVIRONMENT}01"
metastore_admin_group = "databricks-platform-admins"
EOF
  log_info "Arquivo de variáveis criado: ${TFVARS_FILE}"
else
  log_info "Arquivo ${TFVARS_FILE} já existe — não sobrescrevendo"
fi

# ---- instala extensão Databricks no az CLI ----
if ! az extension show --name databricks &>/dev/null; then
  log_info "Instalando extensão databricks no az CLI..."
  az extension add --name databricks
fi

# ---- próximos passos ----
log_info ""
log_info "=========================================="
log_info "Bootstrap Azure concluído!"
log_info "Próximos passos:"
log_info "  1. source ${TFVARS_FILE%.tfvars.*}/.env.${ENVIRONMENT}"
log_info "  2. cd terraform/azure"
log_info "  3. terraform init"
log_info "  4. terraform plan -var-file=terraform.tfvars.${ENVIRONMENT}"
log_info "  5. terraform apply -var-file=terraform.tfvars.${ENVIRONMENT}"
log_info "=========================================="
