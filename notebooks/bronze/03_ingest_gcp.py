# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Ingestão GCP (GCS + BigQuery)
# MAGIC
# MAGIC Ingestão de dados do Google Cloud Storage e de tabelas BigQuery via
# MAGIC spark-bigquery-connector. No GCP, a autenticação usa Workload Identity Federation —
# MAGIC nenhuma chave de serviço (JSON key) é armazenada no cluster ou no workspace.
# MAGIC
# MAGIC O connector do BigQuery lê os dados diretamente pela Storage API, que é muito mais
# MAGIC rápido que a query API para volumes grandes (não passa pelo slot do BigQuery).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("gcs_bucket",       "gs://lakehouse-leandro-prod",    "GCS Bucket")
dbutils.widgets.text("gcs_prefix_raw",   "raw/transacoes/",                "Prefixo GCS raw")
dbutils.widgets.text("gcs_ckpt",         "gs://lakehouse-leandro-prod/checkpoints/bronze/gcp/", "Checkpoint GCS")
dbutils.widgets.text("bq_project",       "meu-projeto-gcp",                "GCP Project BQ")
dbutils.widgets.text("bq_dataset",       "dados_transacionais",            "BigQuery Dataset")
dbutils.widgets.text("bq_table",         "transacoes_historico",           "BigQuery Tabela")
dbutils.widgets.text("catalog",          "lakehouse_prod",                 "Unity Catalog")
dbutils.widgets.text("schema_bronze",    "bronze",                         "Schema bronze")
dbutils.widgets.text("tabela_destino",   "transacoes_raw_gcp",             "Tabela destino Delta")

gcs_bucket      = dbutils.widgets.get("gcs_bucket")
gcs_prefix_raw  = dbutils.widgets.get("gcs_prefix_raw")
gcs_ckpt        = dbutils.widgets.get("gcs_ckpt")
bq_project      = dbutils.widgets.get("bq_project")
bq_dataset      = dbutils.widgets.get("bq_dataset")
bq_table        = dbutils.widgets.get("bq_table")
catalog         = dbutils.widgets.get("catalog")
schema_bronze   = dbutils.widgets.get("schema_bronze")
tabela_destino  = dbutils.widgets.get("tabela_destino")

source_gcs = f"{gcs_bucket}/{gcs_prefix_raw}"
tabela_delta = f"{catalog}.{schema_bronze}.{tabela_destino}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração de credenciais GCP
# MAGIC
# MAGIC Com Workload Identity Federation, o cluster assume a service account via token
# MAGIC federado — não precisa de arquivo de chave JSON. O connector lê automaticamente
# MAGIC as credenciais do metadata server da VM (que roda no GCP).
# MAGIC
# MAGIC Se o ambiente for híbrido (rodar fora do GCP), configurar:
# MAGIC   spark.conf.set("credentials", dbutils.secrets.get("gcp-scope", "sa-key"))
# MAGIC
# MAGIC Aqui assumimos que o workspace Databricks GCP tem a SA com roles corretas:
# MAGIC   - roles/bigquery.dataViewer
# MAGIC   - roles/storage.objectViewer  (no bucket de raw)
# MAGIC   - roles/storage.objectAdmin   (no bucket de lakehouse)

# COMMAND ----------

# configurações do spark para o connector BigQuery
spark.conf.set("viewsEnabled",           "true")
spark.conf.set("materializationProject", bq_project)
spark.conf.set("materializationDataset", bq_dataset)

# temporaryGcsBucket é necessário quando lendo views ou usando filtros complexos
spark.conf.set("temporaryGcsBucket", gcs_bucket.replace("gs://", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de arquivo GCS (streaming incremental)
# MAGIC
# MAGIC Para arquivos no GCS, o Auto Loader funciona com o prefixo gs://.
# MAGIC A autenticação é herdada da service account do cluster.

# COMMAND ----------

schema_base = StructType([
    StructField("id_transacao",  StringType(),    nullable=False),
    StructField("id_cliente",    StringType(),    nullable=True),
    StructField("valor",         DoubleType(),    nullable=True),
    StructField("dt_transacao",  TimestampType(), nullable=True),
    StructField("produto",       StringType(),    nullable=True),
    StructField("canal",         StringType(),    nullable=True),
    StructField("status",        StringType(),    nullable=True),
])

# leitura incremental do GCS com Auto Loader
df_gcs = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", gcs_ckpt + "_schema/")
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
         .schema(schema_base)
         .load(source_gcs)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura do BigQuery (batch histórico)
# MAGIC
# MAGIC Lê tabela histórica do BigQuery para carga inicial (backfill).
# MAGIC Usa a BigQuery Storage API — leitura paralela sem passar pelo BQ query engine.
# MAGIC
# MAGIC Para ingestão incremental do BQ, uma abordagem comum é usar uma view com filtro
# MAGIC de data e reprocessar as últimas N horas. O streaming direto do BQ ainda é limitado.

# COMMAND ----------

df_bq_historico = (
    spark.read
         .format("bigquery")
         .option("project",  bq_project)
         .option("dataset",  bq_dataset)
         .option("table",    bq_table)
         # lê apenas transações dos últimos 90 dias para o backfill inicial
         # em produção isso seria parametrizado pelo orchestrador
         .option("filter",   "dt_transacao >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)")
         .load()
)

print(f"Registros lidos do BigQuery: {df_bq_historico.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona metadados e escreve na Bronze

# COMMAND ----------

# metadados — mesma convenção das outras clouds
df_bq_com_meta = df_bq_historico.select(
    "*",
    F.lit("bigquery").alias("_source_file"),    # não tem nome de arquivo, usa o sistema de origem
    F.current_timestamp().alias("_ingestion_time"),
    F.lit("gcp").alias("_cloud_origem"),
)

# carga inicial do histórico do BigQuery
(df_bq_com_meta
 .write
 .format("delta")
 .mode("overwrite")    # overwrite só na carga inicial
 .option("overwriteSchema", "true")
 .saveAsTable(tabela_delta))

print(f"Carga inicial BigQuery concluída: {spark.table(tabela_delta).count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream incremental do GCS (append de novos arquivos)

# COMMAND ----------

def processa_batch_gcs(batch_df, batch_id):
    validos = batch_df.filter(
        F.col("id_transacao").isNotNull() & F.col("valor").isNotNull()
    ).select(
        "*",
        F.input_file_name().alias("_source_file"),
        F.current_timestamp().alias("_ingestion_time"),
        F.lit("gcp").alias("_cloud_origem"),
    )

    if validos.count() > 0:
        (validos.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(tabela_delta))

    print(f"[GCS] batch={batch_id} | inseridos={validos.count():,}")


query = (
    df_gcs
    .writeStream
    .foreachBatch(processa_batch_gcs)
    .option("checkpointLocation", gcs_ckpt)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# conferência final
spark.table(tabela_delta).groupBy("_cloud_origem").count().show()
