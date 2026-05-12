# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Ingestão AWS (S3)
# MAGIC
# MAGIC Leitura incremental de dados brutos armazenados no S3 via Structured Streaming.
# MAGIC Diferente do Azure (que usa cloudFiles nativo do Databricks), no S3 usamos
# MAGIC o conector S3 configurado via instance profile — sem chave de acesso em texto plano.
# MAGIC
# MAGIC O checkpoint fica também no S3 para garantir que o estado do stream sobrevive
# MAGIC reinicializações do cluster.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("bucket",          "s3-lakehouse-leandro-prod",  "S3 Bucket")
dbutils.widgets.text("prefix_raw",      "raw/transacoes/",            "Prefixo S3 origem")
dbutils.widgets.text("prefix_bronze",   "bronze/transacoes/",         "Prefixo S3 destino")
dbutils.widgets.text("prefix_ckpt",     "checkpoints/bronze/transacoes/", "Prefixo checkpoint")
dbutils.widgets.text("catalog",         "lakehouse_prod",             "Catalog UC")
dbutils.widgets.text("schema_bronze",   "bronze",                     "Schema bronze")
dbutils.widgets.text("tabela_destino",  "transacoes_raw_aws",         "Tabela destino")

bucket         = dbutils.widgets.get("bucket")
prefix_raw     = dbutils.widgets.get("prefix_raw")
prefix_bronze  = dbutils.widgets.get("prefix_bronze")
prefix_ckpt    = dbutils.widgets.get("prefix_ckpt")
catalog        = dbutils.widgets.get("catalog")
schema_bronze  = dbutils.widgets.get("schema_bronze")
tabela_destino = dbutils.widgets.get("tabela_destino")

source_path     = f"s3://{bucket}/{prefix_raw}"
bronze_path     = f"s3://{bucket}/{prefix_bronze}"
checkpoint_path = f"s3://{bucket}/{prefix_ckpt}"

print(f"Lendo de: {source_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração S3
# MAGIC
# MAGIC No S3, a autenticação acontece via instance profile configurado no cluster
# MAGIC (ou na policy do workspace E2). Não precisamos passar credenciais aqui.
# MAGIC
# MAGIC Diferença importante vs ADLS:
# MAGIC - ADLS → abfss:// + OAuth2 (service principal ou Managed Identity)
# MAGIC - S3   → s3:// + IAM role (instance profile ou cross-account role)
# MAGIC
# MAGIC A listagem de arquivos novos é feita via SQS notifications configuradas no bucket.
# MAGIC Isso evita o listing full do bucket a cada trigger, que seria lento em buckets grandes.
# MAGIC
# MAGIC TODO: configurar SQS notification trigger no Terraform e ligar aqui via
# MAGIC       cloudFiles.useNotifications = true quando migrar pra Auto Loader.

# COMMAND ----------

# schema da fonte — igual ao Azure para manter consistência cross-cloud
schema_transacoes = StructType([
    StructField("id_transacao",  StringType(),    nullable=False),
    StructField("id_cliente",    StringType(),    nullable=True),
    StructField("valor",         DoubleType(),    nullable=True),
    StructField("dt_transacao",  TimestampType(), nullable=True),
    StructField("produto",       StringType(),    nullable=True),
    StructField("canal",         StringType(),    nullable=True),
    StructField("status",        StringType(),    nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura Structured Streaming do S3
# MAGIC
# MAGIC Lê arquivos Parquet novos do S3. O Structured Streaming rastreia quais arquivos
# MAGIC já foram processados no checkpoint — sem reprocessamento duplicado.
# MAGIC
# MAGIC No S3 usamos parquet (não JSON) porque o pipeline upstream já entrega parquet
# MAGIC comprimido com snappy. Isso reduz custo de egress na leitura.

# COMMAND ----------

df_raw_s3 = (
    spark.readStream
         .format("cloudFiles")           # Auto Loader funciona no S3 também
         .option("cloudFiles.format",    "parquet")
         .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
         # usa listagem por SQS se disponível — mais eficiente que full listing
         # .option("cloudFiles.useNotifications", "true")
         # .option("cloudFiles.queueUrl", "https://sqs.us-east-1.amazonaws.com/...")
         .schema(schema_transacoes)
         .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadados de ingestão
# MAGIC
# MAGIC Mesmo padrão do pipeline Azure: _source_file e _ingestion_time.
# MAGIC Isso permite rastrear de qual arquivo um registro veio, independente da cloud.

# COMMAND ----------

df_com_metadata = df_raw_s3.select(
    "*",
    F.input_file_name().alias("_source_file"),
    F.current_timestamp().alias("_ingestion_time"),
    F.lit("aws").alias("_cloud_origem"),    # identifica a cloud de origem nos dados
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita Delta no S3 + Unity Catalog

# COMMAND ----------

tabela_completa   = f"{catalog}.{schema_bronze}.{tabela_destino}"
tabela_quarentena = f"{catalog}.{schema_bronze}.{tabela_destino}_quarantine"

def processa_batch_s3(batch_df, batch_id):
    """
    Processa micro-batch do S3.
    Registros inválidos vão pra quarentena.
    Registros válidos append na tabela bronze.
    """
    condicao_ok = (
        F.col("id_transacao").isNotNull()
        & F.col("valor").isNotNull()
        & (F.col("valor") >= 0)
    )

    validos   = batch_df.filter(condicao_ok)
    invalidos = batch_df.filter(~condicao_ok).withColumn(
        "_motivo_quarentena",
        F.when(F.col("id_transacao").isNull(), "id_transacao nulo")
         .when(F.col("valor").isNull(),        "valor nulo")
         .when(F.col("valor") < 0,             "valor negativo")
         .otherwise("outros")
    )

    cnt_ok  = validos.count()
    cnt_bad = invalidos.count()

    if cnt_ok > 0:
        (validos.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(tabela_completa))

    if cnt_bad > 0:
        (invalidos.write
                  .format("delta")
                  .mode("append")
                  .saveAsTable(tabela_quarentena))

    print(f"[S3] batch={batch_id} | ok={cnt_ok:,} | quarentena={cnt_bad:,}")


query = (
    df_com_metadata
    .writeStream
    .foreachBatch(processa_batch_s3)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem final

# COMMAND ----------

total = spark.table(tabela_completa).count()
print(f"Bronze AWS — total registros: {total:,}")
