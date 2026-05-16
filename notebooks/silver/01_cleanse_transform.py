# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Limpeza e Transformação (SCD Type 2)
# MAGIC
# MAGIC Este notebook lê da Bronze, faz deduplicação, padroniza campos e aplica
# MAGIC SCD Tipo 2 via Delta MERGE na camada Silver.
# MAGIC
# MAGIC **O que é SCD Tipo 2?**
# MAGIC Slowly Changing Dimension Type 2 mantém histórico completo de alterações:
# MAGIC quando um registro muda (ex: cliente troca de segmento), a linha antiga
# MAGIC é "fechada" (is_current = false, dt_fim preenchido) e uma nova linha é
# MAGIC inserida como versão atual. Isso permite análises históricas precisas.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("catalog",          "lakehouse_prod",         "Unity Catalog")
dbutils.widgets.text("schema_bronze",    "bronze",                 "Schema Bronze")
dbutils.widgets.text("schema_silver",    "silver",                 "Schema Silver")
dbutils.widgets.text("tabela_bronze",    "transacoes_raw",         "Tabela Bronze origem")
dbutils.widgets.text("tabela_silver",    "transacoes",             "Tabela Silver destino")
dbutils.widgets.text("tabela_clientes",  "clientes",               "Tabela clientes Silver")

catalog         = dbutils.widgets.get("catalog")
schema_bronze   = dbutils.widgets.get("schema_bronze")
schema_silver   = dbutils.widgets.get("schema_silver")
tabela_bronze   = dbutils.widgets.get("tabela_bronze")
tabela_silver   = dbutils.widgets.get("tabela_silver")
tabela_clientes = dbutils.widgets.get("tabela_clientes")

fqn_bronze   = f"{catalog}.{schema_bronze}.{tabela_bronze}"
fqn_silver   = f"{catalog}.{schema_silver}.{tabela_silver}"
fqn_clientes = f"{catalog}.{schema_silver}.{tabela_clientes}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura da Bronze
# MAGIC
# MAGIC Lê apenas registros novos usando _ingestion_time maior que o último processamento.
# MAGIC Em produção isso seria controlado por um watermark numa tabela de controle.
# MAGIC
# MAGIC TODO: implementar tabela de controle de watermark para processos incrementais
# MAGIC       mais robustos — hoje usa sempre a última hora como janela.

# COMMAND ----------

from datetime import datetime, timedelta

# janela de processamento: última 1 hora (em produção seria controlado externamente)
watermark = datetime.now() - timedelta(hours=1)

df_bronze = (
    spark.table(fqn_bronze)
         .filter(F.col("_ingestion_time") >= F.lit(watermark))
)

print(f"Registros lidos da Bronze (última hora): {df_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicação
# MAGIC
# MAGIC Pode existir duplicata na Bronze por reenvio da fonte ou reprocessamento.
# MAGIC Usamos Window + row_number para manter apenas o registro mais recente
# MAGIC de cada id_transacao dentro do batch atual.

# COMMAND ----------

# particiona por id_transacao e ordena pelo _ingestion_time mais recente
janela_dedup = Window.partitionBy("id_transacao").orderBy(F.col("_ingestion_time").desc())

df_dedup = (
    df_bronze
    .withColumn("rn", F.row_number().over(janela_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"Após deduplicação: {df_dedup.count():,} registros únicos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Padronização de strings e tipagem

# COMMAND ----------

df_padronizado = (
    df_dedup
    # remove espaços extras e coloca maiúsculo nos campos categóricos
    .withColumn("produto", F.upper(F.trim(F.col("produto"))))
    .withColumn("canal",   F.upper(F.trim(F.col("canal"))))
    .withColumn("status",  F.upper(F.trim(F.col("status"))))

    # normaliza o id do cliente — remove caracteres especiais
    .withColumn("id_cliente",
        F.regexp_replace(F.col("id_cliente"), "[^A-Za-z0-9]", "")
    )

    # garante que valor tem 2 casas decimais (arredonda)
    .withColumn("valor", F.round(F.col("valor"), 2))

    # extrai campos de data para facilitar particionamento e filtros
    .withColumn("ano_transacao",  F.year(F.col("dt_transacao")))
    .withColumn("mes_transacao",  F.month(F.col("dt_transacao")))
    .withColumn("dia_transacao",  F.dayofmonth(F.col("dt_transacao")))
    .withColumn("hora_transacao", F.hour(F.col("dt_transacao")))

    # classificação de valor por faixa — útil para agregações na Gold
    .withColumn("faixa_valor",
        F.when(F.col("valor") < 100,   F.lit("baixo"))
         .when(F.col("valor") < 1000,  F.lit("medio"))
         .when(F.col("valor") < 10000, F.lit("alto"))
         .otherwise(F.lit("premium"))
    )

    # flag de canal digital vs presencial
    .withColumn("is_digital",
        F.col("canal").isin("APP", "WEB", "API").cast("boolean")
    )

    # data de início de vigência para SCD Type 2
    .withColumn("dt_inicio",  F.col("dt_transacao"))
    .withColumn("dt_fim",     F.lit(None).cast("timestamp"))    # null = registro atual
    .withColumn("is_current", F.lit(True))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Tipo 2 — Delta MERGE
# MAGIC
# MAGIC Aqui está o coração da Silver: o MERGE do Delta Lake.
# MAGIC
# MAGIC **Lógica do MERGE:**
# MAGIC 1. WHEN MATCHED AND is_current = true AND dados mudaram:
# MAGIC    → fecha o registro antigo (is_current = false, dt_fim = now)
# MAGIC    → insere novo registro com dados atualizados (is_current = true)
# MAGIC
# MAGIC 2. WHEN NOT MATCHED:
# MAGIC    → insere o registro como novo (primeira vez que aparece)
# MAGIC
# MAGIC O truque é que o Delta MERGE não faz insert da linha nova na mesma passagem
# MAGIC do close. Por isso usamos o padrão "union + merge" descrito abaixo:
# MAGIC - Primeiro: MERGE para fechar linhas antigas
# MAGIC - Depois: INSERT das novas versões

# COMMAND ----------

# verifica se a tabela Silver já existe; se não, cria com o primeiro batch
tabela_silver_existe = spark._jvm.org.apache.spark.sql.delta.catalog.DeltaCatalog
try:
    delta_silver = DeltaTable.forName(spark, fqn_silver)
    silver_existe = True
    print("Tabela Silver existe — executando MERGE (SCD Type 2)")
except Exception:
    silver_existe = False
    print("Tabela Silver não existe — criando com carga inicial")

# COMMAND ----------

if not silver_existe:
    # carga inicial: escreve tudo como is_current = True
    (df_padronizado
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("ano_transacao", "mes_transacao")
     .saveAsTable(fqn_silver))
    print(f"Carga inicial Silver: {df_padronizado.count():,} registros")

else:
    # --- SCD Type 2 via MERGE ---
    delta_silver = DeltaTable.forName(spark, fqn_silver)

    # monta o dataset de "atualizações" — registros do batch que já existem na Silver
    # e cujos campos de negócio mudaram
    df_novos = df_padronizado.alias("novos")

    # campos que, se mudarem, disparam um novo registro histórico
    campos_negocio = ["status", "faixa_valor", "canal", "produto"]

    # condição de mudança: qualquer campo de negócio diferente
    cond_mudou = " OR ".join(
        [f"antigo.{c} != novos.{c}" for c in campos_negocio]
    )

    (
        delta_silver.alias("antigo")
        .merge(
            df_novos,
            condition=f"antigo.id_transacao = novos.id_transacao AND antigo.is_current = true"
        )
        # QUANDO JÁ EXISTE e DADOS MUDARAM → fecha o registro antigo
        .whenMatchedUpdate(
            condition=cond_mudou,
            set={
                "is_current": "false",
                "dt_fim":     "novos.dt_transacao",
            }
        )
        # QUANDO NÃO EXISTE → insere como novo
        .whenNotMatchedInsertAll()
        .execute()
    )

    # insere as novas versões dos registros que foram fechados no passo anterior
    df_novas_versoes = (
        df_novos
        .join(
            spark.table(fqn_silver).filter(F.col("is_current") == False).select("id_transacao"),
            on="id_transacao",
            how="inner"
        )
    )

    if df_novas_versoes.count() > 0:
        (df_novas_versoes
         .write
         .format("delta")
         .mode("append")
         .saveAsTable(fqn_silver))

    print("MERGE SCD Type 2 executado com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação pós-MERGE

# COMMAND ----------

df_silver = spark.table(fqn_silver)
total          = df_silver.count()
total_corrente = df_silver.filter(F.col("is_current") == True).count()
total_historico = df_silver.filter(F.col("is_current") == False).count()

print(f"Silver — total: {total:,} | correntes: {total_corrente:,} | histórico: {total_historico:,}")

df_silver.groupBy("faixa_valor", "is_current").count().orderBy("faixa_valor", "is_current").show()
