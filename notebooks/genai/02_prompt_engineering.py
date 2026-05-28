# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI — Técnicas de Prompt Engineering
# MAGIC
# MAGIC Demonstração prática das principais técnicas de prompt engineering usando
# MAGIC os Foundation Model APIs do Databricks (DBRX Instruct).
# MAGIC
# MAGIC Técnicas cobertas:
# MAGIC 1. **Zero-shot:** pergunta direta sem exemplos
# MAGIC 2. **Few-shot:** fornece exemplos antes da pergunta
# MAGIC 3. **Chain-of-Thought (CoT):** instrui o modelo a raciocinar passo a passo
# MAGIC 4. **Structured Output:** força saída em JSON via prompt
# MAGIC 5. **Role Prompting:** define persona específica para o LLM

# COMMAND ----------

# MAGIC %pip install langchain langchain-community

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import os
import json
from langchain_community.chat_models import ChatDatabricks
from langchain.schema import HumanMessage, SystemMessage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup do LLM

# COMMAND ----------

databricks_host  = spark.conf.get("spark.databricks.workspaceUrl")
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

os.environ["DATABRICKS_HOST"]  = f"https://{databricks_host}"
os.environ["DATABRICKS_TOKEN"] = databricks_token

llm = ChatDatabricks(
    endpoint="databricks-dbrx-instruct",
    temperature=0.2,
    max_tokens=1024,
)

def chamar_llm(messages: list, label: str = "") -> str:
    """Chama o LLM e imprime o resultado formatado."""
    resposta = llm.invoke(messages).content
    if label:
        print(f"\n{'='*60}")
        print(f"[{label}]")
        print(f"{'='*60}")
        print(resposta)
    return resposta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 1 — Zero-shot
# MAGIC
# MAGIC Faz a pergunta sem fornecer nenhum exemplo. Funciona bem quando a tarefa
# MAGIC é simples ou o modelo foi treinado para aquele tipo de resposta.
# MAGIC Limitação: o modelo pode interpretar a tarefa de formas inesperadas.

# COMMAND ----------

# exemplo: classificação de sentimento sem exemplos
prompt_zero_shot = """Classifique o sentimento do texto abaixo como POSITIVO, NEGATIVO ou NEUTRO.

Texto: "O produto chegou no prazo, mas a embalagem estava amassada. O atendimento resolveu rápido."

Sentimento:"""

chamar_llm([HumanMessage(content=prompt_zero_shot)], label="ZERO-SHOT — Sentimento")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 2 — Few-shot
# MAGIC
# MAGIC Fornece exemplos de entrada e saída antes da pergunta real.
# MAGIC Guia o modelo sobre o formato e o raciocínio esperado.
# MAGIC
# MAGIC Regra geral: 3 a 5 exemplos bem escolhidos são suficientes.
# MAGIC Exemplos devem cobrir casos diferentes (positivo, negativo, neutro).

# COMMAND ----------

prompt_few_shot = """Classifique o sentimento do texto como POSITIVO, NEGATIVO ou NEUTRO.

Exemplo 1:
Texto: "Adorei o produto! Chegou antes do prazo e a qualidade é excelente."
Sentimento: POSITIVO

Exemplo 2:
Texto: "Péssimo atendimento. Produto com defeito e ninguém resolve."
Sentimento: NEGATIVO

Exemplo 3:
Texto: "Recebi o produto. É exatamente o que estava descrito."
Sentimento: NEUTRO

Agora classifique:
Texto: "O produto chegou no prazo, mas a embalagem estava amassada. O atendimento resolveu rápido."
Sentimento:"""

chamar_llm([HumanMessage(content=prompt_few_shot)], label="FEW-SHOT — Sentimento (3 exemplos)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 3 — Chain-of-Thought (CoT)
# MAGIC
# MAGIC Instrui o modelo a "pensar em voz alta" antes de dar a resposta final.
# MAGIC Melhora muito a precisão em tarefas de raciocínio, matemática e lógica.
# MAGIC
# MAGIC Basta adicionar "Pense passo a passo" ou "Let's think step by step" ao prompt.
# MAGIC O modelo começa a externalizar o raciocínio intermediário, o que reduz erros.

# COMMAND ----------

# exemplo: problema de negócio com raciocínio necessário
prompt_cot = """Um cliente comprou R$1.200 em produtos no mês passado e tem histórico de
compra há 3 anos. Nos últimos 45 dias, não realizou nenhuma transação.
O modelo RFM atribuiu: R=2, F=5, M=5. Score total: 255.

Pense passo a passo para determinar:
1. Qual é a situação atual desse cliente segundo o RFM?
2. Qual ação de retenção é mais indicada?
3. Qual deveria ser a oferta personalizada?

Raciocínio e conclusão:"""

chamar_llm([HumanMessage(content=prompt_cot)], label="CHAIN-OF-THOUGHT — Análise RFM cliente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 4 — Structured Output (JSON)
# MAGIC
# MAGIC Formata o prompt para que o LLM retorne JSON válido.
# MAGIC Essencial quando o output vai ser consumido por código downstream.
# MAGIC
# MAGIC Dicas para structured output:
# MAGIC - Especifique o schema exato no prompt
# MAGIC - Peça para retornar APENAS o JSON, sem texto antes ou depois
# MAGIC - Use temperature baixa (0.0 a 0.1) para consistência
# MAGIC - Valide com json.loads() e trate exceções

# COMMAND ----------

llm_deterministic = ChatDatabricks(
    endpoint="databricks-dbrx-instruct",
    temperature=0.0,    # zero para máxima consistência no JSON
    max_tokens=512,
)

prompt_json = """Analise o perfil do cliente abaixo e retorne APENAS um objeto JSON válido,
sem texto antes ou depois, com exatamente esta estrutura:

{
  "segmento": "<Campeao|Leal|Regular|Em_Risco|Inativo|Novo>",
  "score_risco_churn": <0.0 a 1.0>,
  "acoes_recomendadas": ["<acao1>", "<acao2>", "<acao3>"],
  "canal_preferencial": "<Email|SMS|Push|Telefonema>",
  "urgencia": "<Alta|Media|Baixa>"
}

Perfil do cliente:
- Última compra: 45 dias atrás
- Frequência (12m): 24 compras
- Valor total (12m): R$ 8.400
- Score RFM: 255 (R=2, F=5, M=5)
- Canal histórico: App (70%), Web (30%)

JSON:"""

resposta_json = llm_deterministic.invoke([HumanMessage(content=prompt_json)]).content

print("\n[STRUCTURED OUTPUT — JSON]")
print(resposta_json)

# valida se é JSON válido
try:
    dados = json.loads(resposta_json.strip())
    print("\nJSON válido! Dados parseados:")
    print(json.dumps(dados, indent=2, ensure_ascii=False))
except json.JSONDecodeError as e:
    print(f"AVISO: JSON inválido — {e}")
    # TODO: adicionar retry com instrução de correção para JSONDecodeError

# COMMAND ----------

# MAGIC %md
# MAGIC ## Técnica 5 — Role Prompting (System Message)
# MAGIC
# MAGIC Define uma persona/papel específico para o LLM via System Message.
# MAGIC O modelo adapta tom, vocabulário e foco conforme o papel definido.
# MAGIC
# MAGIC Útil quando o mesmo LLM precisa atender públicos diferentes
# MAGIC (analista técnico vs executivo C-level).

# COMMAND ----------

# persona 1: analista técnico de dados
system_tecnico = SystemMessage(content="""Você é um engenheiro de dados sênior especializado
em Databricks e Delta Lake. Suas respostas são técnicas, precisas e usam terminologia correta
de dados. Quando relevante, mencione comandos SQL ou PySpark.""")

pergunta = HumanMessage(content="Explique em 3 bullets o que é Z-ORDER no Delta Lake e quando usar.")

chamar_llm([system_tecnico, pergunta], label="ROLE — Engenheiro de Dados")

# COMMAND ----------

# persona 2: executivo de negócios
system_executivo = SystemMessage(content="""Você é um consultor de estratégia de dados para C-level.
Suas respostas evitam jargão técnico, focam em valor de negócio e impacto financeiro.
Use linguagem executiva, clara e orientada a resultados.""")

chamar_llm([system_executivo, pergunta], label="ROLE — Executivo C-Level")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação de Outputs
# MAGIC
# MAGIC A tabela abaixo resume quando usar cada técnica:
# MAGIC
# MAGIC | Técnica | Quando Usar | Trade-off |
# MAGIC |---|---|---|
# MAGIC | Zero-shot | Tarefas simples e diretas | Pode ser impreciso em tarefas complexas |
# MAGIC | Few-shot | Quando o formato de saída importa | Ocupa mais tokens de contexto |
# MAGIC | Chain-of-Thought | Raciocínio, lógica, matemática | Resposta mais longa, maior custo |
# MAGIC | Structured Output | Integração com sistemas/APIs | Precisa validar e tratar erros de formato |
# MAGIC | Role Prompting | Múltiplos públicos-alvo | Não garante que o LLM siga o papel 100% |

# COMMAND ----------

print("Notebook de Prompt Engineering concluído.")
print("Técnicas demonstradas: Zero-shot | Few-shot | Chain-of-Thought | Structured Output | Role Prompting")
