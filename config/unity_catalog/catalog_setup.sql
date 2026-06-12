-- =============================================================================
-- Unity Catalog — Setup de Catalogs, Schemas e Permissões
-- Projeto: databricks-ml-platform-three-clouds
-- Executar como: metastore admin ou account admin
-- =============================================================================

-- ============================================================
-- 1. CATALOGS
-- ============================================================

-- catalog de produção
CREATE CATALOG IF NOT EXISTS lakehouse_prod
  COMMENT 'Catalog principal de produção — dados certificados e modelos ML';

-- catalog de desenvolvimento (isolado do prod)
CREATE CATALOG IF NOT EXISTS lakehouse_dev
  COMMENT 'Catalog de desenvolvimento — dados e experimentos dos engenheiros';

-- catalog de staging para homologação
CREATE CATALOG IF NOT EXISTS lakehouse_staging
  COMMENT 'Catalog de staging — validação antes de promover para prod';

-- ============================================================
-- 2. SCHEMAS dentro do catalog prod
-- ============================================================

USE CATALOG lakehouse_prod;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Dados brutos ingeridos — sem transformação, apenas metadados adicionados'
  WITH DBPROPERTIES (
    'data_classification' = 'internal',
    'pii_present'         = 'possible',
    'retention_days'      = '90'
  );

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Dados limpos e padronizados — deduplicados, SCD Type 2 aplicado'
  WITH DBPROPERTIES (
    'data_classification' = 'internal',
    'pii_present'         = 'possible',
    'retention_days'      = '365'
  );

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Dados agregados e prontos para consumo — dashboards, relatórios e ML'
  WITH DBPROPERTIES (
    'data_classification' = 'internal',
    'pii_present'         = 'false',
    'retention_days'      = '730'
  );

CREATE SCHEMA IF NOT EXISTS ml
  COMMENT 'Tabelas de feature store e resultados de modelos ML'
  WITH DBPROPERTIES (
    'data_classification' = 'internal',
    'retention_days'      = '365'
  );

CREATE SCHEMA IF NOT EXISTS genai
  COMMENT 'Índices de Vector Search e tabelas de base de conhecimento RAG'
  WITH DBPROPERTIES (
    'data_classification' = 'internal',
    'retention_days'      = '365'
  );

CREATE SCHEMA IF NOT EXISTS data_quality
  COMMENT 'Resultados de validações Great Expectations e alertas de qualidade'
  WITH DBPROPERTIES (
    'data_classification' = 'internal',
    'retention_days'      = '180'
  );

-- ============================================================
-- 3. PERMISSÕES — USE CATALOG
-- ============================================================

-- grupo de engenheiros de dados: acesso completo ao dev, leitura no prod
GRANT USE CATALOG ON CATALOG lakehouse_prod    TO `data-engineers`;
GRANT USE CATALOG ON CATALOG lakehouse_dev     TO `data-engineers`;
GRANT USE CATALOG ON CATALOG lakehouse_staging TO `data-engineers`;

-- grupo de cientistas de dados: leitura no prod, acesso completo ao dev
GRANT USE CATALOG ON CATALOG lakehouse_prod TO `data-scientists`;
GRANT USE CATALOG ON CATALOG lakehouse_dev  TO `data-scientists`;

-- grupo de analistas: apenas prod, apenas schemas gold e data_quality
GRANT USE CATALOG ON CATALOG lakehouse_prod TO `data-analysts`;

-- admins da plataforma: tudo
GRANT ALL PRIVILEGES ON CATALOG lakehouse_prod     TO `databricks-platform-admins`;
GRANT ALL PRIVILEGES ON CATALOG lakehouse_dev      TO `databricks-platform-admins`;
GRANT ALL PRIVILEGES ON CATALOG lakehouse_staging  TO `databricks-platform-admins`;

-- ============================================================
-- 4. PERMISSÕES — SCHEMAS
-- ============================================================

USE CATALOG lakehouse_prod;

-- engenheiros de dados: leitura na bronze e silver, escrita na gold (via jobs)
GRANT USE SCHEMA, SELECT ON SCHEMA bronze TO `data-engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `data-engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA gold TO `data-engineers`;

-- cientistas de dados: leitura em tudo, escrita no schema ml
GRANT USE SCHEMA, SELECT ON SCHEMA bronze TO `data-scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `data-scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `data-scientists`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MODEL ON SCHEMA ml TO `data-scientists`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA genai TO `data-scientists`;

-- analistas: apenas gold e data_quality (sem PII da bronze/silver)
GRANT USE SCHEMA, SELECT ON SCHEMA gold         TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA data_quality TO `data-analysts`;

-- service principals dos jobs: acesso total nos schemas necessários
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA bronze       TO `sp-etl-prod`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA silver       TO `sp-etl-prod`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA gold         TO `sp-etl-prod`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA data_quality TO `sp-etl-prod`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MODEL ON SCHEMA ml TO `sp-ml-prod`;

-- ============================================================
-- 5. TABELAS DE EXEMPLO COM TAGS
-- ============================================================

-- tabela principal de transações — Silver
CREATE TABLE IF NOT EXISTS lakehouse_prod.silver.transacoes (
  id_transacao    STRING  NOT NULL COMMENT 'Identificador único da transação',
  id_cliente      STRING           COMMENT 'ID do cliente',
  valor           DOUBLE           COMMENT 'Valor da transação em BRL',
  dt_transacao    TIMESTAMP        COMMENT 'Data e hora da transação',
  produto         STRING           COMMENT 'Produto/serviço transacionado',
  canal           STRING           COMMENT 'Canal de venda (APP, WEB, LOJA, etc)',
  status          STRING           COMMENT 'Status da transação',
  faixa_valor     STRING           COMMENT 'Classificação de valor: baixo/medio/alto/premium',
  is_digital      BOOLEAN          COMMENT 'True se canal digital (APP, WEB, API)',
  ano_transacao   INT              COMMENT 'Ano — coluna de partição',
  mes_transacao   INT              COMMENT 'Mês — coluna de partição',
  dia_transacao   INT              COMMENT 'Dia do mês',
  hora_transacao  INT              COMMENT 'Hora da transação',
  dt_inicio       TIMESTAMP        COMMENT 'Início de vigência (SCD Type 2)',
  dt_fim          TIMESTAMP        COMMENT 'Fim de vigência — NULL = registro atual (SCD Type 2)',
  is_current      BOOLEAN          COMMENT 'True = versão corrente do registro (SCD Type 2)',
  _source_file    STRING           COMMENT 'Arquivo de origem (rastreabilidade)',
  _ingestion_time TIMESTAMP        COMMENT 'Timestamp de ingestão no lakehouse',
  _cloud_origem   STRING           COMMENT 'Cloud de onde o dado foi ingerido'
)
USING DELTA
PARTITIONED BY (ano_transacao, mes_transacao)
COMMENT 'Tabela Silver de transações — dados limpos com SCD Type 2'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'quality'                          = 'silver',
  'pii_fields'                       = 'id_cliente',
  'data_owner'                       = 'data-engineering-team',
  'sla_availability'                 = '99.5'
);

-- tabela de RFM — Gold
CREATE TABLE IF NOT EXISTS lakehouse_prod.gold.rfm_clientes (
  id_cliente   STRING  NOT NULL COMMENT 'Identificador do cliente',
  recency_dias INT              COMMENT 'Dias desde última compra',
  frequency    BIGINT           COMMENT 'Quantidade de compras nos últimos 12 meses',
  monetary     DOUBLE           COMMENT 'Valor total comprado nos últimos 12 meses',
  r_score      INT              COMMENT 'Score Recency 1-5',
  f_score      INT              COMMENT 'Score Frequency 1-5',
  m_score      INT              COMMENT 'Score Monetary 1-5',
  rfm_score    INT              COMMENT 'Score RFM combinado (R*100+F*10+M)',
  segmento     STRING           COMMENT 'Segmento do cliente: Campeao, Leal, Regular, Em_Risco, Inativo, Novo',
  dt_calculo   DATE             COMMENT 'Data de referência do cálculo RFM'
)
USING DELTA
COMMENT 'Scores RFM por cliente — calculados diariamente'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality'                          = 'gold',
  'pii_fields'                       = 'none',
  'data_owner'                       = 'data-analytics-team',
  'refresh_frequency'                = 'daily'
);

-- ============================================================
-- 6. EXTERNAL LOCATIONS (para apontar para ADLS/S3/GCS)
-- ============================================================

-- ADLS Gen2 — Azure
CREATE EXTERNAL LOCATION IF NOT EXISTS adls_raw_azure
  URL 'abfss://raw@stleandroprod01.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `ac-unity-catalog-prod`)
  COMMENT 'Storage externo — container RAW do ADLS Gen2 (Azure)';

-- S3 — AWS
CREATE EXTERNAL LOCATION IF NOT EXISTS s3_lakehouse_aws
  URL 's3://s3-lakehouse-leandro-prod/'
  WITH (STORAGE CREDENTIAL `aws-databricks-storage-credential`)
  COMMENT 'Storage externo — bucket S3 do lakehouse (AWS)';

-- GCS — GCP
CREATE EXTERNAL LOCATION IF NOT EXISTS gcs_lakehouse_gcp
  URL 'gs://lakehouse-leandro-prod/'
  WITH (STORAGE CREDENTIAL `gcp-databricks-storage-credential`)
  COMMENT 'Storage externo — bucket GCS do lakehouse (GCP)';

-- ============================================================
-- 7. STORAGE CREDENTIALS
-- ============================================================

-- credencial para ADLS (usa o Access Connector com Managed Identity)
CREATE STORAGE CREDENTIAL IF NOT EXISTS `ac-unity-catalog-prod`
  WITH AZURE_MANAGED_IDENTITY (
    ACCESS_CONNECTOR_ID '/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/rg-databricks-prod/providers/Microsoft.Databricks/accessConnectors/ac-unity-catalog-prod'
  )
  COMMENT 'Credencial Managed Identity para ADLS Gen2 — Unity Catalog';

-- ============================================================
-- 8. AUDIT — View para monitorar acesso a tabelas com PII
-- ============================================================

CREATE OR REPLACE VIEW lakehouse_prod.data_quality.v_audit_pii_access AS
SELECT
  user_identity.email AS usuario,
  event_time,
  request_params.table_full_name AS tabela,
  action_name,
  source_ip_address
FROM system.access.audit
WHERE action_name IN ('selectFromTable', 'readFromTable')
  AND request_params.table_full_name IN (
    'lakehouse_prod.silver.transacoes',
    'lakehouse_prod.bronze.transacoes_raw'
  )
ORDER BY event_time DESC;

COMMENT ON VIEW lakehouse_prod.data_quality.v_audit_pii_access
  IS 'Auditoria de acesso às tabelas que contêm campos PII (id_cliente)';
