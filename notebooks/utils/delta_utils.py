# Databricks notebook source
# MAGIC %md
# MAGIC # Utilitários Delta Lake
# MAGIC
# MAGIC Funções auxiliares para operações de manutenção e gerenciamento de tabelas Delta:
# MAGIC - OPTIMIZE com Z-ORDER (compacta pequenos arquivos e melhora leitura por filtro)
# MAGIC - VACUUM (remove arquivos antigos sem referência)
# MAGIC - CLONE (cria ambientes isolados dev/staging/prod)
# MAGIC - Histórico e restore de versão (viagem no tempo)
# MAGIC
# MAGIC Este módulo é importado pelos outros notebooks via %run ou como utilitário
# MAGIC no job de manutenção semanal.

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from typing import Optional, List
import datetime


# pega a SparkSession ativa — funciona tanto em notebook quanto em job
spark = SparkSession.getActiveSession()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE — compactação e Z-ORDER

# COMMAND ----------

def optimize_table(
    tabela_fqn: str,
    zorder_cols: Optional[List[str]] = None,
    particao_where: Optional[str] = None
) -> None:
    """
    Executa OPTIMIZE em uma tabela Delta.
    Opcionalmente aplica Z-ORDER para otimizar queries com filtros específicos.

    Args:
        tabela_fqn:     nome completo da tabela (catalog.schema.tabela)
        zorder_cols:    lista de colunas para Z-ORDER (ex: ["id_cliente", "dt_ref"])
        particao_where: cláusula WHERE para otimizar só uma partição (ex: "ano=2024 AND mes=3")

    Exemplos:
        optimize_table("prod.gold.receita_diaria", ["id_cliente", "dt_ref"])
        optimize_table("prod.silver.transacoes",   ["id_cliente"], "ano=2024 AND mes=3")
    """
    sql_optimize = f"OPTIMIZE {tabela_fqn}"

    if particao_where:
        sql_optimize += f" WHERE {particao_where}"

    if zorder_cols:
        cols_str = ", ".join(zorder_cols)
        sql_optimize += f" ZORDER BY ({cols_str})"

    print(f"[OPTIMIZE] {tabela_fqn}")
    if zorder_cols:
        print(f"  Z-ORDER: {zorder_cols}")
    if particao_where:
        print(f"  Partição: {particao_where}")

    inicio = datetime.datetime.now()
    resultado = spark.sql(sql_optimize)
    duracao = (datetime.datetime.now() - inicio).seconds

    # extrai estatísticas do resultado do OPTIMIZE
    stats = resultado.select(
        "metrics.numFilesAdded",
        "metrics.numFilesRemoved",
        "metrics.filesAdded.avg",
        "metrics.numOutputBytes"
    ).collect()

    if stats:
        s = stats[0]
        print(f"  Arquivos adicionados: {s[0]} | removidos: {s[1]} | tempo: {duracao}s")
    else:
        print(f"  Concluído em {duracao}s")


# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM — limpeza de arquivos antigos

# COMMAND ----------

def vacuum_table(
    tabela_fqn: str,
    retention_hours: int = 168,    # padrão Delta: 7 dias (168h)
    dry_run: bool = True
) -> None:
    """
    Remove arquivos Delta sem referência (arquivos de versões antigas, arquivos de dados
    que não fazem mais parte de nenhuma versão do Delta log).

    ATENÇÃO: retention_hours abaixo de 168h (7 dias) pode quebrar leituras concorrentes
    e time travel. Só reduzir se tiver certeza que não há leituras em andamento.

    Args:
        tabela_fqn:      nome completo da tabela
        retention_hours: horas de retenção (default 168 = 7 dias)
        dry_run:         se True, apenas lista os arquivos que seriam removidos (não apaga)

    Exemplos:
        vacuum_table("prod.bronze.transacoes_raw", dry_run=True)   # ver antes de apagar
        vacuum_table("prod.bronze.transacoes_raw", dry_run=False)  # apaga de verdade
    """
    if retention_hours < 168:
        print(f"AVISO: retention_hours={retention_hours} está abaixo do mínimo recomendado (168h).")
        print("Habilitando spark.databricks.delta.retentionDurationCheck.enabled=false")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    modo = "DRY RUN" if dry_run else "REAL"
    print(f"[VACUUM {modo}] {tabela_fqn} — retenção: {retention_hours}h")

    if dry_run:
        result = spark.sql(f"VACUUM {tabela_fqn} RETAIN {retention_hours} HOURS DRY RUN")
        arquivos = result.count()
        print(f"  Arquivos que seriam removidos: {arquivos:,}")
    else:
        spark.sql(f"VACUUM {tabela_fqn} RETAIN {retention_hours} HOURS")
        print("  VACUUM concluído")

    # restaura configuração padrão
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")


# COMMAND ----------

# MAGIC %md
# MAGIC ## CLONE — cópia de ambiente (dev/staging/prod)

# COMMAND ----------

def clone_table(
    tabela_origem: str,
    tabela_destino: str,
    tipo: str = "SHALLOW",
    versao: Optional[int] = None
) -> None:
    """
    Cria um clone de tabela Delta para isolamento de ambiente.

    SHALLOW CLONE: copia apenas os metadados e o Delta log — não copia os arquivos
    de dados. Qualquer escrita no clone cria novos arquivos no destino. Ideal para
    testes rápidos sem duplicar storage.

    DEEP CLONE: copia todos os arquivos de dados + metadados. Totalmente independente
    da origem. Ideal para backups ou promoção entre ambientes.

    Args:
        tabela_origem:  tabela fonte
        tabela_destino: tabela destino
        tipo:           "SHALLOW" (padrão) ou "DEEP"
        versao:         versão específica para clonar (None = última versão)

    Exemplos:
        # shallow para dev (rápido, sem duplicar dados)
        clone_table("prod.gold.rfm_clientes", "dev.gold.rfm_clientes", "SHALLOW")

        # deep clone para backup
        clone_table("prod.gold.rfm_clientes", "backup.gold.rfm_clientes_20240301", "DEEP")
    """
    if tipo not in ("SHALLOW", "DEEP"):
        raise ValueError(f"tipo deve ser SHALLOW ou DEEP, recebido: {tipo}")

    versao_str = f"VERSION AS OF {versao}" if versao is not None else ""

    sql_clone = (
        f"CREATE OR REPLACE TABLE {tabela_destino} "
        f"{tipo} CLONE {tabela_origem} {versao_str}"
    )

    print(f"[CLONE {tipo}] {tabela_origem} → {tabela_destino}")
    if versao is not None:
        print(f"  Versão: {versao}")

    spark.sql(sql_clone)
    count_dest = spark.table(tabela_destino).count()
    print(f"  Clone criado — {count_dest:,} registros")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Histórico e Restore de Versão (Time Travel)

# COMMAND ----------

def historico_tabela(tabela_fqn: str, ultimas_n: int = 10) -> None:
    """
    Exibe o histórico de operações de uma tabela Delta.
    Útil para auditoria e para identificar versões para restore.

    Args:
        tabela_fqn: nome completo da tabela
        ultimas_n:  quantas versões recentes exibir (default 10)
    """
    print(f"[HISTÓRICO] {tabela_fqn} — últimas {ultimas_n} versões")
    (spark.sql(f"DESCRIBE HISTORY {tabela_fqn}")
          .select("version", "timestamp", "operation", "operationParameters",
                  "operationMetrics.numOutputRows", "userName")
          .orderBy(F.col("version").desc())
          .limit(ultimas_n)
          .show(truncate=50))


def restore_tabela(tabela_fqn: str, versao: Optional[int] = None, timestamp: Optional[str] = None) -> None:
    """
    Restaura uma tabela Delta para uma versão ou timestamp anterior.
    ATENÇÃO: operação irreversível. Sempre faça um clone antes de restaurar em produção.

    Args:
        tabela_fqn: nome completo da tabela
        versao:     número da versão (ex: 5)
        timestamp:  timestamp da versão (ex: "2024-03-01 10:00:00")

    Exemplos:
        restore_tabela("prod.gold.rfm_clientes", versao=5)
        restore_tabela("prod.gold.rfm_clientes", timestamp="2024-03-01 10:00:00")
    """
    if versao is None and timestamp is None:
        raise ValueError("Informe versao ou timestamp para o restore")

    if versao is not None:
        ref = f"VERSION AS OF {versao}"
    else:
        ref = f"TIMESTAMP AS OF '{timestamp}'"

    print(f"[RESTORE] {tabela_fqn} → {ref}")
    spark.sql(f"RESTORE TABLE {tabela_fqn} TO {ref}")
    count = spark.table(tabela_fqn).count()
    print(f"  Restore concluído — {count:,} registros após restore")


def ler_versao_historica(tabela_fqn: str, versao: int):
    """
    Lê uma versão específica de uma tabela Delta sem alterar a versão atual.
    Útil para comparar dados entre versões.

    Args:
        tabela_fqn: nome completo da tabela
        versao:     versão a ler

    Returns:
        DataFrame da versão solicitada
    """
    return spark.read.format("delta").option("versionAsOf", versao).table(tabela_fqn)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Manutenção programada de todas as tabelas Gold

# COMMAND ----------

def manutencao_gold(catalog: str, schema_gold: str = "gold") -> None:
    """
    Executa OPTIMIZE + VACUUM nas tabelas Gold de forma programada.
    Chamada pelo job de manutenção semanal nos Databricks Workflows.
    """
    tabelas_gold = {
        f"{catalog}.{schema_gold}.receita_diaria": ["id_cliente", "dt_ref"],
        f"{catalog}.{schema_gold}.rfm_clientes":   ["segmento", "rfm_score"],
        f"{catalog}.{schema_gold}.kpis_executivos": [],
    }

    print(f"=== Manutenção Gold — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} ===\n")

    for tabela, zorder in tabelas_gold.items():
        optimize_table(tabela, zorder_cols=zorder if zorder else None)
        vacuum_table(tabela, retention_hours=168, dry_run=False)
        print()

    print("=== Manutenção concluída ===")


# se executado diretamente como notebook (não importado), roda a manutenção
if __name__ == "__main__":
    catalog = dbutils.widgets.get("catalog") if "dbutils" in dir() else "lakehouse_prod"
    manutencao_gold(catalog)
