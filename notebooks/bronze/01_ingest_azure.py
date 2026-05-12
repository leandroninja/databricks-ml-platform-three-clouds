# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Ingestão Azure (ADLS Gen2)
# MAGIC
# MAGIC Notebook responsável pela ingestão incremental de dados brutos do ADLS Gen2
# MAGIC usando Auto Loader com cloudFiles. O schema é inferido e evolui automaticamente
# MAGIC conforme novos campos chegam na fonte.

# COMMAND ----------

# importações padrão
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, DoubleType
)
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros do notebook
# MAGIC
# MAGIC Esses valores podem ser sobrescritos via Databricks Widgets quando o notebook
# MAGIC é chamado por um job ou workflow.

# COMMAND ----------

dbutils.widgets.text("storage_account", "stleandroprod01", "Storage Account")
dbutils.widgets.text("container", "raw", "Container ADLS")
dbutils.widgets.text("source_folder", "transacoes/", "Pasta de origem")
dbutils.widgets.text("catalog", "lakehouse_prod", "Unity Catalog")
dbutils.widgets.text("schema_bronze", "bronze", "Schema Bronze")
dbutils.widgets.text("tabela_destino", "transacoes_raw", "Tabela destino")

storage_account = dbutils.widgets.get("storage_account")
container       = dbutils.widgets.get("container")
source_folder   = dbutils.widgets.get("source_folder")
catalog         = dbutils.widgets.get("catalog")
schema_bronze   = dbutils.widgets.get("schema_bronze")
tabela_destino  = dbutils.widgets.get("tabela_destino")

# caminho completo no ADLS Gen2 (abfss)
source_path     = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{source_folder}"
checkpoint_path = f"abfss://checkpoints@{storage_account}.dfs.core.windows.net/bronze/{tabela_destino}"
quarantine_path = f"abfss://quarantine@{storage_account}.dfs.core.windows.net/bronze/{tabela_destino}"

print(f"Lendo de: {source_path}")
print(f"Checkpoint: {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema esperado
# MAGIC
# MAGIC Define o schema inicial. O Auto Loader vai fazer merge automático se novos
# MAGIC campos chegarem (cloudFiles.schemaEvolutionMode = addNewColumns).

# COMMAND ----------

# schema base — campos obrigatórios que devem existir sempre
schema_base = StructType([
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
# MAGIC ## Leitura com Auto Loader
# MAGIC
# MAGIC cloudFiles.format = json   → lê arquivos JSON que chegam no ADLS
# MAGIC cloudFiles.schemaEvolutionMode = addNewColumns → permite schema drift sem quebrar
# MAGIC cloudFiles.inferColumnTypes = true → tenta inferir tipos automaticamente
# MAGIC
# MAGIC O Auto Loader mantém estado no checkpoint e reprocessa apenas arquivos novos.
# MAGIC Em produção, o trigger é definido como AvailableNow para rodar em batch incremental.

# COMMAND ----------

df_raw = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
         .option("cloudFiles.inferColumnTypes", "true")
         # rescue column guarda campos que não casam com o schema — útil pra debug
         .option("cloudFiles.rescuedDataColumn", "_rescue")
         .schema(schema_base)
         .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona metadados de ingestão
# MAGIC
# MAGIC Campos técnicos adicionados em toda camada Bronze:
# MAGIC - _source_file: nome do arquivo de origem (rastreabilidade)
# MAGIC - _ingestion_time: timestamp de quando o registro entrou no lakehouse

# COMMAND ----------

df_com_metadata = df_raw.select(
    "*",
    F.input_file_name().alias("_source_file"),
    F.current_timestamp().alias("_ingestion_time"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separação de registros válidos e quarentena
# MAGIC
# MAGIC Registros sem id_transacao ou com valor nulo são enviados para a quarentena.
# MAGIC A quarentena fica numa tabela Delta separada para análise posterior.
# MAGIC
# MAGIC TODO: adicionar validação de formato do id_transacao (regex UUID) quando
# MAGIC a equipe de dados definir o padrão definitivo.

# COMMAND ----------

# condição de validade — pode ser expandida conforme regras de negócio evoluem
condicao_valido = (
    F.col("id_transacao").isNotNull()
    & F.col("valor").isNotNull()
    & (F.col("valor") >= 0)
)

df_valido     = df_com_metadata.filter(condicao_valido)
df_quarentena = df_com_metadata.filter(~condicao_valido).withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_transacao").isNull(), F.lit("id_transacao nulo"))
     .when(F.col("valor").isNull(),        F.lit("valor nulo"))
     .when(F.col("valor") < 0,             F.lit("valor negativo"))
     .otherwise(F.lit("outros"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita na tabela Bronze (Delta)

# COMMAND ----------

def escreve_bronze(batch_df, batch_id):
    """
    Processa cada micro-batch:
    1. Validos → tabela bronze principal (append)
    2. Inválidos → tabela quarentena (append)
    """
    # filtra válidos e inválidos dentro do micro-batch
    validos = batch_df.filter(
        F.col("id_transacao").isNotNull()
        & F.col("valor").isNotNull()
        & (F.col("valor") >= 0)
    )

    invalidos = batch_df.filter(
        F.col("id_transacao").isNull()
        | F.col("valor").isNull()
        | (F.col("valor") < 0)
    ).withColumn(
        "_motivo_quarentena",
        F.when(F.col("id_transacao").isNull(), F.lit("id_transacao nulo"))
         .when(F.col("valor").isNull(),        F.lit("valor nulo"))
         .when(F.col("valor") < 0,             F.lit("valor negativo"))
         .otherwise(F.lit("outros"))
    )

    tabela_completa    = f"{catalog}.{schema_bronze}.{tabela_destino}"
    tabela_quarentena  = f"{catalog}.{schema_bronze}.{tabela_destino}_quarantine"

    # escrita na bronze
    if validos.count() > 0:
        (validos.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")    # permite schema evolution no Delta
                .saveAsTable(tabela_completa))

    # escrita na quarentena
    if invalidos.count() > 0:
        (invalidos.write
                  .format("delta")
                  .mode("append")
                  .saveAsTable(tabela_quarentena))

    print(f"batch_id={batch_id} | válidos={validos.count()} | quarentena={invalidos.count()}")


# stream principal usando foreachBatch para ter controle granular
query = (
    df_com_metadata
    .writeStream
    .foreachBatch(escreve_bronze)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)    # batch incremental — processa todos os arquivos novos e para
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado

# COMMAND ----------

tabela_completa = f"{catalog}.{schema_bronze}.{tabela_destino}"
df_resultado = spark.table(tabela_completa)

print(f"Total de registros na bronze: {df_resultado.count():,}")
df_resultado.show(5, truncate=False)
