#!/usr/bin/env bash
# =============================================================================
# bootstrap_aws.sh — Setup inicial do ambiente Databricks na AWS
# Leandro Oliveira Moraes
#
# Pré-requisitos:
#   - AWS CLI >= 2.x configurada com perfil válido (aws configure)
#   - Terraform >= 1.6
#   - Databricks CLI >= 0.200
#   - Permissões: IAMFullAccess, S3FullAccess, VPCFullAccess, EC2FullAccess
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# ---- parâmetros ----
ENVIRONMENT="${ENVIRONMENT:-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="${PROJECT_NAME:-databricks-ml-platform}"
AWS_PROFILE="${AWS_PROFILE:-default}"

S3_STATE_BUCKET="s3-terraform-state-leandro"
DYNAMO_TABLE="terraform-state-lock"

# ---- validações ----
log_info "Verificando dependências..."

for dep in aws terraform databricks jq; do
  if ! command -v "$dep" &>/dev/null; then
    log_error "Dependência não encontrada: $dep"
    exit 1
  fi
  log_info "  $dep OK"
done

# ---- verifica autenticação AWS ----
log_info "Verificando credenciais AWS (perfil: ${AWS_PROFILE})..."

if ! AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity &>/dev/null; then
  log_error "Credenciais AWS inválidas. Execute: aws configure --profile ${AWS_PROFILE}"
  exit 1
fi

ACCOUNT_ID=$(AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity --query Account --output text)
log_info "Account ID: ${ACCOUNT_ID} | Região: ${AWS_REGION}"

export AWS_PROFILE
export AWS_DEFAULT_REGION="$AWS_REGION"

# ---- cria bucket S3 para Terraform state ----
log_info "Criando bucket S3 para Terraform state..."

if aws s3api head-bucket --bucket "$S3_STATE_BUCKET" 2>/dev/null; then
  log_info "Bucket '${S3_STATE_BUCKET}' já existe"
else
  if [[ "$AWS_REGION" == "us-east-1" ]]; then
    # us-east-1 não aceita LocationConstraint
    aws s3api create-bucket \
      --bucket "$S3_STATE_BUCKET" \
      --region "$AWS_REGION"
  else
    aws s3api create-bucket \
      --bucket "$S3_STATE_BUCKET" \
      --region "$AWS_REGION" \
      --create-bucket-configuration LocationConstraint="$AWS_REGION"
  fi

  aws s3api put-bucket-versioning \
    --bucket "$S3_STATE_BUCKET" \
    --versioning-configuration Status=Enabled

  aws s3api put-bucket-encryption \
    --bucket "$S3_STATE_BUCKET" \
    --server-side-encryption-configuration '{
      "Rules": [{
        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
        "BucketKeyEnabled": true
      }]
    }'

  aws s3api put-public-access-block \
    --bucket "$S3_STATE_BUCKET" \
    --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

  log_info "Bucket S3 de state criado e configurado"
fi

# ---- cria tabela DynamoDB para lock do state ----
log_info "Configurando DynamoDB para lock do Terraform state..."

if aws dynamodb describe-table --table-name "$DYNAMO_TABLE" &>/dev/null; then
  log_info "Tabela DynamoDB '${DYNAMO_TABLE}' já existe"
else
  aws dynamodb create-table \
    --table-name "$DYNAMO_TABLE" \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "$AWS_REGION"

  log_info "Tabela DynamoDB '${DYNAMO_TABLE}' criada"
fi

# ---- gera terraform.tfvars ----
TFVARS_FILE="terraform/aws/terraform.tfvars.${ENVIRONMENT}"

if [[ ! -f "$TFVARS_FILE" ]]; then
  # o nome do bucket S3 do lakehouse precisa ser único globalmente
  LAKEHOUSE_BUCKET="s3-lakehouse-${PROJECT_NAME//[-_]/-}-${ENVIRONMENT}-${ACCOUNT_ID}"

  cat > "$TFVARS_FILE" <<EOF
# gerado por bootstrap_aws.sh
project_name   = "${PROJECT_NAME}"
environment    = "${ENVIRONMENT}"
aws_region     = "${AWS_REGION}"
s3_bucket_name = "${LAKEHOUSE_BUCKET}"
vpc_cidr       = "10.0.0.0/16"

# preencher manualmente após criar conta Databricks E2:
databricks_account_id    = ""
databricks_client_id     = ""
databricks_client_secret = ""
EOF

  log_info "Arquivo de variáveis criado: ${TFVARS_FILE}"
  log_warn "ATENÇÃO: preencha databricks_account_id, databricks_client_id e databricks_client_secret em ${TFVARS_FILE}"
else
  log_info "Arquivo ${TFVARS_FILE} já existe"
fi

# ---- próximos passos ----
log_info ""
log_info "=========================================="
log_info "Bootstrap AWS concluído!"
log_info "Próximos passos:"
log_info "  1. Preencha as vars Databricks em ${TFVARS_FILE}"
log_info "  2. cd terraform/aws"
log_info "  3. terraform init"
log_info "  4. terraform plan -var-file=terraform.tfvars.${ENVIRONMENT}"
log_info "  5. terraform apply -var-file=terraform.tfvars.${ENVIRONMENT}"
log_info "=========================================="
