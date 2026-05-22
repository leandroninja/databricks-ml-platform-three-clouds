# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality — Verificações com Great Expectations
# MAGIC
# MAGIC Valida a qualidade dos dados nas camadas Silver e Gold usando Great Expectations.
# MAGIC Os resultados são salvos em Delta para histórico e alertas são disparados
# MAGIC quando a qualidade cai abaixo do threshold definido por tabela.
# MAGIC
# MAGIC Great Expectations usa o conceito de "Expectations" — regras declarativas
# MAGIC sobre como os dados devem ser. Ex: "coluna X não pode ter nulos",
# MAGIC "coluna Y deve estar entre 0 e 100".

# COMMAND ----------

# MAGIC %pip install great-expectations

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

from pyspark.sql import functions as F
import datetime
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("catalog",          "lakehouse_prod",   "Unity Catalog")
dbutils.widgets.text("schema_silver",    "silver",           "Schema Silver")
dbutils.widgets.text("schema_gold",      "gold",             "Schema Gold")
dbutils.widgets.text("schema_dq",        "data_quality",     "Schema DQ")
dbutils.widgets.text("threshold_pct",    "95",               "Threshold mínimo DQ (%)")

catalog       = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold   = dbutils.widgets.get("schema_gold")
schema_dq     = dbutils.widgets.get("schema_dq")
threshold_pct = float(dbutils.widgets.get("threshold_pct"))

fqn_silver      = f"{catalog}.{schema_silver}.transacoes"
fqn_rfm         = f"{catalog}.{schema_gold}.rfm_clientes"
fqn_receita     = f"{catalog}.{schema_gold}.receita_diaria"
fqn_dq_resultado = f"{catalog}.{schema_dq}.dq_resultados"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicializa Great Expectations com DataContext

# COMMAND ----------

# usa o data context em memória (sem precisar de filesystem externo)
context = gx.get_context(mode="ephemeral")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função auxiliar — cria e valida Expectation Suite

# COMMAND ----------

def valida_dataframe(df_spark, nome_suite: str, expectations: list) -> dict:
    """
    Valida um DataFrame Spark contra uma lista de expectations.
    Retorna dicionário com resultados por expectation.

    Args:
        df_spark:     DataFrame Spark a validar
        nome_suite:   nome da suite (identificador)
        expectations: lista de funções que recebem o validator e adicionam expectations
    Returns:
        dict com resultados da validação
    """
    # converte para pandas para usar com GE (datasets menores)
    # para datasets grandes, usar GE com Spark nativo via SparkDFDataset
    df_pd = df_spark.toPandas()

    suite = context.add_expectation_suite(
        expectation_suite_name=nome_suite,
        overwrite_existing=True
    )

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name=nome_suite,
            data_connector_name="runtime_data_connector",
            data_asset_name=nome_suite,
            runtime_parameters={"batch_data": df_pd},
            batch_identifiers={"run_id": str(datetime.datetime.now())},
        ),
        expectation_suite_name=nome_suite,
    )

    # aplica cada expectation
    for fn_expectation in expectations:
        fn_expectation(validator)

    # valida e retorna resultados
    resultado = validator.validate()
    return resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations — Silver (transacoes)

# COMMAND ----------

df_silver = spark.table(fqn_silver).filter(F.col("is_current") == True)

def expectations_silver(v):
    """Define as regras de qualidade para a tabela Silver de transações."""

    # campos obrigatórios não podem ter nulo
    v.expect_column_values_to_not_be_null("id_transacao")
    v.expect_column_values_to_not_be_null("id_cliente")
    v.expect_column_values_to_not_be_null("valor")
    v.expect_column_values_to_not_be_null("dt_transacao")
    v.expect_column_values_to_not_be_null("is_current")

    # id_transacao deve ser único (sem duplicatas)
    v.expect_column_values_to_be_unique("id_transacao")

    # valor deve ser positivo
    v.expect_column_values_to_be_between("valor", min_value=0.01, max_value=1_000_000)

    # status deve ser um valor do domínio conhecido
    v.expect_column_values_to_be_in_set(
        "status", ["APROVADO", "REPROVADO", "CONCLUIDO", "CANCELADO", "PENDENTE"]
    )

    # canal deve ser do domínio esperado
    v.expect_column_values_to_be_in_set(
        "canal", ["APP", "WEB", "API", "LOJA", "TELEFONE", "PARCEIRO"]
    )

    # faixa_valor deve ser preenchida
    v.expect_column_values_to_not_be_null("faixa_valor")
    v.expect_column_values_to_be_in_set(
        "faixa_valor", ["baixo", "medio", "alto", "premium"]
    )

    # id_cliente não deve ter menos de 5 caracteres (detecta dados truncados)
    v.expect_column_value_lengths_to_be_between("id_cliente", min_value=5, max_value=50)

    # dt_transacao deve estar dentro de um range razoável
    v.expect_column_values_to_be_between(
        "dt_transacao",
        min_value="2020-01-01",
        max_value=str(datetime.date.today()),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations — Gold (rfm_clientes)

# COMMAND ----------

df_rfm = spark.table(fqn_rfm)

def expectations_rfm(v):
    """Regras de qualidade para a tabela de RFM."""

    v.expect_column_values_to_not_be_null("id_cliente")
    v.expect_column_values_to_not_be_null("segmento")
    v.expect_column_values_to_not_be_null("rfm_score")

    # scores RFM devem estar entre 1 e 5
    for score_col in ["r_score", "f_score", "m_score"]:
        v.expect_column_values_to_be_between(score_col, min_value=1, max_value=5)

    # rfm_score = R*100 + F*10 + M → range 111 a 555
    v.expect_column_values_to_be_between("rfm_score", min_value=111, max_value=555)

    # segmentos válidos
    v.expect_column_values_to_be_in_set(
        "segmento",
        ["Campeao", "Leal", "Regular", "Em_Risco", "Inativo", "Novo"]
    )

    # recency_dias não pode ser negativo
    v.expect_column_values_to_be_between("recency_dias", min_value=0, max_value=3650)

    # monetary não pode ser negativo
    v.expect_column_values_to_be_between("monetary", min_value=0)

    # frequency deve ser pelo menos 1
    v.expect_column_values_to_be_between("frequency", min_value=1)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução das validações e coleta de resultados

# COMMAND ----------

resultados_dq = []
data_exec = datetime.datetime.now()

def processa_resultado(resultado_ge, tabela: str) -> dict:
    """Extrai métricas do resultado do GE e monta dicionário para Delta."""
    total       = resultado_ge.statistics["evaluated_expectations"]
    sucesso     = resultado_ge.statistics["successful_expectations"]
    falha       = resultado_ge.statistics["unsuccessful_expectations"]
    pct_sucesso = sucesso / total * 100 if total > 0 else 0

    falhas_detalhe = [
        {
            "expectation": r.expectation_config.expectation_type,
            "coluna":      r.expectation_config.kwargs.get("column", ""),
            "sucesso":     r.success,
        }
        for r in resultado_ge.results
        if not r.success
    ]

    return {
        "tabela":            tabela,
        "dt_execucao":       data_exec,
        "total_checks":      total,
        "checks_ok":         sucesso,
        "checks_falha":      falha,
        "pct_qualidade":     round(pct_sucesso, 2),
        "passou_threshold":  pct_sucesso >= threshold_pct,
        "falhas_json":       json.dumps(falhas_detalhe, ensure_ascii=False),
    }


# valida Silver
print("Validando Silver — transacoes...")
res_silver = valida_dataframe(df_silver, "silver_transacoes", [expectations_silver])
resultados_dq.append(processa_resultado(res_silver, fqn_silver))

# valida Gold RFM
print("Validando Gold — rfm_clientes...")
res_rfm = valida_dataframe(df_rfm, "gold_rfm", [expectations_rfm])
resultados_dq.append(processa_resultado(res_rfm, fqn_rfm))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva resultados em Delta e verifica alertas

# COMMAND ----------

df_resultado = spark.createDataFrame(resultados_dq)

(df_resultado
 .write
 .format("delta")
 .mode("append")
 .saveAsTable(fqn_dq_resultado))

print("\n--- Resultado das validações ---")
for r in resultados_dq:
    status = "OK" if r["passou_threshold"] else "ALERTA"
    print(f"[{status}] {r['tabela']}: {r['pct_qualidade']}% ({r['checks_ok']}/{r['total_checks']} checks)")
    if not r["passou_threshold"]:
        print(f"  Falhas: {r['falhas_json']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alerta quando qualidade abaixo do threshold

# COMMAND ----------

falhas_criticas = [r for r in resultados_dq if not r["passou_threshold"]]

if falhas_criticas:
    # em produção: chamar API de alertas (PagerDuty, Slack, email via SendGrid)
    # por ora, loga o erro claramente para o Databricks Workflows capturar
    tabelas_problema = [r["tabela"] for r in falhas_criticas]
    raise Exception(
        f"ALERTA DQ: {len(falhas_criticas)} tabela(s) abaixo do threshold de {threshold_pct}%: "
        f"{tabelas_problema}"
    )
else:
    print(f"\nTodas as tabelas passaram no threshold de {threshold_pct}%. DQ OK.")
