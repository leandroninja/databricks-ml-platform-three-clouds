# Databricks notebook source
# fix 2026-04-14: Z-ORDER estava sendo aplicado em coluna errada (customer_id ao invés de order_date)
# MAGIC %md
# MAGIC # Gold — Camada de Negócio (RFM + KPIs)
# MAGIC
# MAGIC Constrói as tabelas da camada Gold consumidas diretamente por dashboards,
# MAGIC relatórios executivos e modelos de ML.
# MAGIC
# MAGIC Tabelas geradas:
# MAGIC - `receita_diaria`     — receita agregada por cliente, produto e dia
# MAGIC - `rfm_clientes`       — score RFM (Recency, Frequency, Monetary) por cliente
# MAGIC - `kpis_executivos`    — MoM e YoY de receita, volume e ticket médio
# MAGIC
# MAGIC Todas as tabelas são escritas em Delta com Z-ORDER para otimizar
# MAGIC as queries mais comuns (filtro por cliente, data, produto).

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("catalog",       "lakehouse_prod", "Unity Catalog")
dbutils.widgets.text("schema_silver", "silver",         "Schema Silver")
dbutils.widgets.text("schema_gold",   "gold",           "Schema Gold")
dbutils.widgets.text("data_ref",      "",               "Data referência (YYYY-MM-DD, vazio = hoje)")

catalog       = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold   = dbutils.widgets.get("schema_gold")
data_ref_str  = dbutils.widgets.get("data_ref")

# usa hoje se não informado
if not data_ref_str:
    data_ref = datetime.date.today()
else:
    data_ref = datetime.date.fromisoformat(data_ref_str)

fqn_silver    = f"{catalog}.{schema_silver}.transacoes"
fqn_receita   = f"{catalog}.{schema_gold}.receita_diaria"
fqn_rfm       = f"{catalog}.{schema_gold}.rfm_clientes"
fqn_kpis      = f"{catalog}.{schema_gold}.kpis_executivos"

print(f"Data referência: {data_ref}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura da Silver (apenas registros correntes)

# COMMAND ----------

df_silver = (
    spark.table(fqn_silver)
         .filter(F.col("is_current") == True)
         .filter(F.col("status").isin("APROVADO", "CONCLUIDO"))    # só transações válidas
)

# cache pois vai ser usado várias vezes neste notebook
df_silver.cache()
print(f"Silver (correntes aprovados): {df_silver.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 1 — Receita Diária por Cliente e Produto
# MAGIC
# MAGIC Granularidade: cliente × produto × data.
# MAGIC É a tabela base para a maioria dos dashboards de receita.

# COMMAND ----------

df_receita = (
    df_silver
    .groupBy(
        F.col("id_cliente"),
        F.col("produto"),
        F.col("ano_transacao").alias("ano"),
        F.col("mes_transacao").alias("mes"),
        F.col("dia_transacao").alias("dia"),
        F.to_date(F.col("dt_transacao")).alias("dt_ref"),
        F.col("canal"),
        F.col("is_digital"),
    )
    .agg(
        F.sum("valor").alias("receita_total"),
        F.count("id_transacao").alias("qtd_transacoes"),
        F.avg("valor").alias("ticket_medio"),
        F.max("valor").alias("maior_transacao"),
        F.min("valor").alias("menor_transacao"),
    )
    .withColumn("receita_total",   F.round("receita_total", 2))
    .withColumn("ticket_medio",    F.round("ticket_medio", 2))
    .withColumn("maior_transacao", F.round("maior_transacao", 2))
    .withColumn("menor_transacao", F.round("menor_transacao", 2))
)

# escrita com overwrite da partição do dia de referência (idempotente)
(df_receita
 .write
 .format("delta")
 .mode("overwrite")
 .option("replaceWhere", f"dt_ref = '{data_ref}'")
 .partitionBy("ano", "mes")
 .saveAsTable(fqn_receita))

print(f"Receita diária gravada: {df_receita.count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 2 — RFM (Recency, Frequency, Monetary)
# MAGIC
# MAGIC O modelo RFM classifica clientes em 3 dimensões:
# MAGIC - **Recency (R):**  quantos dias desde a última compra? (menor = melhor)
# MAGIC - **Frequency (F):** quantas compras fez nos últimos 12 meses? (maior = melhor)
# MAGIC - **Monetary (M):**  quanto gastou nos últimos 12 meses? (maior = melhor)
# MAGIC
# MAGIC Cada dimensão recebe um score de 1 a 5 (quintis).
# MAGIC Score combinado = R*100 + F*10 + M (ex: 555 = cliente campeão).

# COMMAND ----------

data_ref_ts = F.lit(str(data_ref)).cast("date")

# janela de 12 meses
df_12m = df_silver.filter(
    F.datediff(data_ref_ts, F.to_date("dt_transacao")) <= 365
)

# métricas base por cliente
df_rfm_base = (
    df_12m
    .groupBy("id_cliente")
    .agg(
        F.datediff(data_ref_ts, F.max(F.to_date("dt_transacao"))).alias("recency_dias"),
        F.count("id_transacao").alias("frequency"),
        F.sum("valor").alias("monetary"),
    )
    .withColumn("monetary", F.round("monetary", 2))
)

# scores por quintil usando Window + ntile
# recency: invertido — menor recency_dias = score maior
janela_r = Window.orderBy(F.col("recency_dias").desc())    # desc pq menor dias = melhor
janela_f = Window.orderBy(F.col("frequency").asc())
janela_m = Window.orderBy(F.col("monetary").asc())

df_rfm = (
    df_rfm_base
    .withColumn("r_score", F.ntile(5).over(janela_r))    # 1=pior, 5=melhor
    .withColumn("f_score", F.ntile(5).over(janela_f))
    .withColumn("m_score", F.ntile(5).over(janela_m))
    .withColumn("rfm_score",
        F.col("r_score") * 100 + F.col("f_score") * 10 + F.col("m_score")
    )
    # segmentação baseada no RFM score
    .withColumn("segmento",
        F.when(F.col("rfm_score") >= 444, F.lit("Campeao"))
         .when(F.col("rfm_score") >= 333, F.lit("Leal"))
         .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), F.lit("Novo"))
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4), F.lit("Em_Risco"))
         .when(F.col("r_score") <= 2, F.lit("Inativo"))
         .otherwise(F.lit("Regular"))
    )
    .withColumn("dt_calculo", F.lit(str(data_ref)).cast("date"))
)

(df_rfm
 .write
 .format("delta")
 .mode("overwrite")
 .option("replaceWhere", f"dt_calculo = '{data_ref}'")
 .saveAsTable(fqn_rfm))

print(f"RFM calculado para {df_rfm.count():,} clientes")
df_rfm.groupBy("segmento").count().orderBy(F.col("count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 3 — KPIs Executivos (MoM e YoY)
# MAGIC
# MAGIC Receita, volume e ticket médio com comparativos mês-a-mês e ano-a-ano.
# MAGIC Usado nos dashboards executivos C-level.

# COMMAND ----------

# agrega por mês
df_mensal = (
    df_silver
    .groupBy("ano_transacao", "mes_transacao")
    .agg(
        F.sum("valor").alias("receita_mes"),
        F.count("id_transacao").alias("volume_mes"),
        F.avg("valor").alias("ticket_medio_mes"),
        F.countDistinct("id_cliente").alias("clientes_ativos"),
    )
    .withColumn("receita_mes",      F.round("receita_mes", 2))
    .withColumn("ticket_medio_mes", F.round("ticket_medio_mes", 2))
)

# Window para MoM (mês anterior) e YoY (mesmo mês do ano anterior)
janela_lag = Window.orderBy("ano_transacao", "mes_transacao")

df_kpis = (
    df_mensal
    .withColumn("receita_mes_anterior",  F.lag("receita_mes", 1).over(janela_lag))
    .withColumn("receita_mesmo_mes_ano_anterior", F.lag("receita_mes", 12).over(janela_lag))

    # MoM growth %
    .withColumn("mom_receita_pct",
        F.round(
            (F.col("receita_mes") - F.col("receita_mes_anterior"))
            / F.col("receita_mes_anterior") * 100,
            2
        )
    )
    # YoY growth %
    .withColumn("yoy_receita_pct",
        F.round(
            (F.col("receita_mes") - F.col("receita_mesmo_mes_ano_anterior"))
            / F.col("receita_mesmo_mes_ano_anterior") * 100,
            2
        )
    )
    .withColumn("dt_calculo", F.lit(str(data_ref)).cast("date"))
)

(df_kpis
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(fqn_kpis))

print(f"KPIs executivos gravados: {df_kpis.count():,} meses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Otimização das tabelas Gold com Z-ORDER
# MAGIC
# MAGIC Z-ORDER coloca dados relacionados nos mesmos arquivos Parquet,
# MAGIC reduzindo o número de arquivos que o Spark precisa ler numa query filtrada.
# MAGIC
# MAGIC - receita_diaria → Z-ORDER por id_cliente, dt_ref (filtros mais comuns)
# MAGIC - rfm_clientes   → Z-ORDER por segmento, rfm_score
# MAGIC
# MAGIC OPTIMIZE deve rodar após cargas grandes. Para tabelas menores que 10GB,
# MAGIC pode ser agendado semanalmente.

# COMMAND ----------

# otimiza receita diária — mais importante pois é consultada com filtro de cliente
spark.sql(f"OPTIMIZE {fqn_receita} ZORDER BY (id_cliente, dt_ref)")
print("OPTIMIZE receita_diaria concluído")

spark.sql(f"OPTIMIZE {fqn_rfm} ZORDER BY (segmento, rfm_score)")
print("OPTIMIZE rfm_clientes concluído")

spark.sql(f"OPTIMIZE {fqn_kpis}")
print("OPTIMIZE kpis_executivos concluído")

# COMMAND ----------

# resultado rápido para validação
print("\n--- Últimos 3 meses de KPIs ---")
(spark.table(fqn_kpis)
      .orderBy(F.col("ano_transacao").desc(), F.col("mes_transacao").desc())
      .select("ano_transacao", "mes_transacao", "receita_mes",
              "mom_receita_pct", "yoy_receita_pct", "clientes_ativos")
      .show(3))
