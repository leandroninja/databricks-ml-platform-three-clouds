# databricks-ml-platform-three-clouds

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![Terraform](https://img.shields.io/badge/Terraform-1.6%2B-purple?logo=terraform)](https://terraform.io)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red?logo=databricks)](https://databricks.com)
[![Azure](https://img.shields.io/badge/Azure-ADB-0078D4?logo=microsoftazure)](https://azure.microsoft.com)
[![AWS](https://img.shields.io/badge/AWS-DBR-FF9900?logo=amazonaws)](https://aws.amazon.com)
[![GCP](https://img.shields.io/badge/GCP-DBR-4285F4?logo=googlecloud)](https://cloud.google.com)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## Sobre

Plataforma completa Databricks Lakehouse demonstrando arquitetura de dados em produção nas três principais
nuvens — **Azure**, **AWS** e **GCP** — com pipelines Bronze → Silver → Gold, treinamento de modelos ML
com rastreamento MLflow e pipelines de Geração Aumentada por Recuperação (RAG) com Databricks Vector Search
e LangChain.

Este repositório foi construído para demonstrar domínio prático de arquitetura Databricks em ambientes
multi-cloud reais, cobrindo provisionamento de infraestrutura (Terraform), orquestração de workflows,
governança com Unity Catalog e GenAI aplicada a casos de uso corporativos.

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────────┐
│                    FONTES DE DADOS (multi-cloud)                     │
│  ADLS Gen2 (Azure)  │   S3 (AWS)   │   GCS (GCP) + BigQuery         │
└────────────┬─────────────────┬──────────────────┬────────────────────┘
             │                 │                  │
             ▼                 ▼                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         CAMADA BRONZE                                │
│  Auto Loader / Structured Streaming / Schema Evolution               │
│  Quarentena de registros inválidos | Metadados de ingestão           │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         CAMADA SILVER                                │
│  Deduplicação (Window + row_number)                                  │
│  SCD Tipo 2 com Delta MERGE | Padronização e tipagem de campos       │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                          CAMADA GOLD                                 │
│  Receita diária por cliente/produto | RFM (Recency, Frequency, $)   │
│  KPIs executivos (MoM / YoY) | Delta com Z-ORDER                    │
└───────────────────┬──────────────────────┬───────────────────────────┘
                    │                      │
                    ▼                      ▼
┌────────────────────────┐    ┌────────────────────────────────────────┐
│      CAMADA ML         │    │            CAMADA GenAI                │
│  XGBoost Churn Model   │    │  RAG Pipeline (Vector Search)          │
│  Hyperopt + MLflow     │    │  LangChain + DBRX / LLaMA 2           │
│  Model Registry        │    │  Prompt Engineering (zero/few/CoT)    │
└────────────────────────┘    └────────────────────────────────────────┘
                    │                      │
                    ▼                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│              GOVERNANÇA & ORQUESTRAÇÃO                               │
│  Unity Catalog | Databricks Workflows | Great Expectations DQ       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Clouds suportadas

### Azure — Azure Databricks + ADLS Gen2
- Workspace Premium com Private Endpoints (UI + backend)
- Unity Catalog Metastore provisionado via Terraform
- Auto Loader com cloudFiles apontando para ADLS Gen2
- Workload Identity para autenticação sem segredos em texto plano
- Clusters Spot para workloads batch

### AWS — Databricks on AWS (E2) + S3
- Workspace E2 com VPC dedicada e cross-account IAM role
- Instance Profiles para acesso granular ao S3
- Structured Streaming de S3 com checkpoint no Delta
- Subnets privadas com NAT Gateway

### GCP — Databricks on GCP + GCS
- Workspace GCP com VPC nativa
- Workload Identity Federation (sem service account key em disco)
- spark-bigquery-connector para ingestão de tabelas BigQuery
- GCS como storage primário com lifecycle policies

---

## Estrutura do repositório

```
databricks-ml-platform-three-clouds/
├── notebooks/
│   ├── bronze/
│   │   ├── 01_ingest_azure.py       # Auto Loader ADLS Gen2
│   │   ├── 02_ingest_aws.py         # Structured Streaming S3
│   │   └── 03_ingest_gcp.py         # GCS + BigQuery connector
│   ├── silver/
│   │   └── 01_cleanse_transform.py  # Dedup + SCD Type 2
│   ├── gold/
│   │   └── 01_build_gold.py         # RFM + KPIs + Z-ORDER
│   ├── ml/
│   │   └── 01_train_churn_model.py  # XGBoost + Hyperopt + MLflow
│   ├── genai/
│   │   ├── 01_rag_pipeline.py       # RAG com Vector Search
│   │   └── 02_prompt_engineering.py # Zero-shot, few-shot, CoT
│   ├── data_quality/
│   │   └── 01_dq_checks.py          # Great Expectations
│   └── utils/
│       └── delta_utils.py           # OPTIMIZE, VACUUM, CLONE, history
├── terraform/
│   ├── azure/                       # Provider azurerm + databricks
│   ├── aws/                         # Provider aws + databricks
│   └── gcp/                         # Provider google + databricks
├── config/
│   ├── jobs/
│   │   ├── azure_etl_job.json       # Workflow com 5 tasks
│   │   └── ml_training_job.json     # Job com GPU cluster
│   └── unity_catalog/
│       └── catalog_setup.sql        # Catalog, schema, grants
├── scripts/
│   ├── bootstrap_azure.sh
│   ├── bootstrap_aws.sh
│   └── bootstrap_gcp.sh
└── .github/
    └── workflows/
        └── ci.yml                   # Lint + testes
```

---

## Como rodar

### Pré-requisitos
- Python 3.10+
- Terraform 1.6+
- Databricks CLI 0.200+
- Credenciais configuradas para a cloud desejada (az login / aws configure / gcloud auth)

### Azure
```bash
# 1. autenticação
az login
az account set --subscription <SUBSCRIPTION_ID>

# 2. bootstrap (cria resource group, ADLS, service principal)
bash scripts/bootstrap_azure.sh

# 3. infraestrutura
cd terraform/azure
terraform init && terraform plan && terraform apply

# 4. executar job de ETL
databricks jobs run-now --job-id <JOB_ID>
```

### AWS
```bash
aws configure   # ou configure perfil no ~/.aws/credentials
bash scripts/bootstrap_aws.sh
cd terraform/aws && terraform init && terraform apply
```

### GCP
```bash
gcloud auth application-default login
bash scripts/bootstrap_gcp.sh
cd terraform/gcp && terraform init && terraform apply
```

---

## Notebooks incluídos

| Notebook | Camada | Descrição |
|---|---|---|
| `01_ingest_azure.py` | Bronze | Auto Loader com schema evolution e quarentena |
| `02_ingest_aws.py` | Bronze | Structured Streaming de S3 com checkpoint |
| `03_ingest_gcp.py` | Bronze | GCS + BigQuery connector |
| `01_cleanse_transform.py` | Silver | Dedup, SCD Type 2, padronização |
| `01_build_gold.py` | Gold | RFM, KPIs MoM/YoY, Z-ORDER |
| `01_train_churn_model.py` | ML | XGBoost churn + Hyperopt + MLflow Registry |
| `01_rag_pipeline.py` | GenAI | RAG com Vector Search + LangChain |
| `02_prompt_engineering.py` | GenAI | Zero-shot, few-shot, chain-of-thought |
| `01_dq_checks.py` | DQ | Great Expectations + alertas de qualidade |
| `delta_utils.py` | Utils | OPTIMIZE, VACUUM, CLONE, history/restore |

---

## Modelo ML — Churn Prediction

- **Algoritmo:** XGBoost com Hyperopt para busca de hiperparâmetros (TPE)
- **Rastreamento:** MLflow nested runs (experimento pai + trials filhos)
- **Registro:** Melhor modelo promovido automaticamente ao Model Registry
- **Features:** RFM score, histórico de transações, dados demográficos
- **Assinatura:** input/output schema registrado com `mlflow.models.infer_signature`

## Pipeline GenAI — RAG

- **Embeddings:** Databricks Vector Search (índice Delta)
- **LLM:** DBRX Instruct via Databricks Foundation Model APIs (fallback LLaMA 2 70B)
- **Framework:** LangChain com `RetrievalQA` chain
- **Prompt Engineering:** templates estruturados com formatação JSON no output
- **Técnicas demonstradas:** zero-shot, few-shot (3 exemplos), chain-of-thought

---

## Autor

**Leandro Oliveira Moraes**
Arquiteto Sênior DevOps & Multi-Cloud

[![LinkedIn](https://img.shields.io/badge/LinkedIn-leandroninja-0077B5?logo=linkedin)](https://linkedin.com/in/leandroninja)

Certificações Databricks:
- Databricks Certified Platform Architect — Azure
- Databricks Certified Platform Architect — AWS
- Databricks Certified Platform Architect — GCP
- Databricks Certified Generative AI Fundamentals
- Databricks Certified Prompt Engineering Fundamentals
- Databricks Lakehouse Fundamentals

---

## Licença

MIT
