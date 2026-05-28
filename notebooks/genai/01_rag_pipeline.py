# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI — Pipeline RAG com Databricks Vector Search + LangChain
# MAGIC
# MAGIC Implementa um pipeline RAG (Retrieval Augmented Generation) para responder
# MAGIC perguntas sobre a base de conhecimento da empresa usando:
# MAGIC
# MAGIC - **Databricks Vector Search:** índice vetorial Delta para recuperar documentos relevantes
# MAGIC - **DBRX Instruct:** LLM principal (fallback: LLaMA 2 70B via Foundation Model APIs)
# MAGIC - **LangChain:** orquestração da chain RAG (retriever → prompt → LLM → output)
# MAGIC
# MAGIC **O que é RAG?**
# MAGIC Em vez de depender apenas do conhecimento interno do LLM (que tem cutoff de treino
# MAGIC e não conhece dados da empresa), o RAG recupera documentos relevantes da base de
# MAGIC conhecimento e os passa como contexto para o LLM. Isso reduz alucinação e permite
# MAGIC respostas atualizadas com dados privados sem fine-tuning.

# COMMAND ----------

# MAGIC %pip install langchain langchain-community databricks-vectorsearch mlflow

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import os
import mlflow
from databricks.vector_search.client import VectorSearchClient
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_community.chat_models import ChatDatabricks
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain_community.embeddings import DatabricksEmbeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

dbutils.widgets.text("catalog",           "lakehouse_prod",                   "Unity Catalog")
dbutils.widgets.text("schema_genai",      "genai",                            "Schema GenAI")
dbutils.widgets.text("vs_endpoint",       "vs-endpoint-prod",                 "Vector Search endpoint")
dbutils.widgets.text("vs_index",          "lakehouse_prod.genai.docs_index",  "Vector Search index")
dbutils.widgets.text("llm_endpoint",      "databricks-dbrx-instruct",         "LLM endpoint")
dbutils.widgets.text("embedding_endpoint","databricks-bge-large-en",          "Embedding endpoint")
dbutils.widgets.text("k_docs",            "5",                                "Documentos recuperados (k)")

catalog            = dbutils.widgets.get("catalog")
schema_genai       = dbutils.widgets.get("schema_genai")
vs_endpoint        = dbutils.widgets.get("vs_endpoint")
vs_index           = dbutils.widgets.get("vs_index")
llm_endpoint       = dbutils.widgets.get("llm_endpoint")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
k_docs             = int(dbutils.widgets.get("k_docs"))

# token do workspace para autenticar os endpoints
databricks_host  = spark.conf.get("spark.databricks.workspaceUrl")
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

os.environ["DATABRICKS_HOST"]  = f"https://{databricks_host}"
os.environ["DATABRICKS_TOKEN"] = databricks_token

# COMMAND ----------

# MAGIC %md
# MAGIC ## Componente 1 — Embeddings
# MAGIC
# MAGIC Usa o endpoint de embedding do Databricks (BGE Large EN) para converter
# MAGIC texto em vetores. O mesmo modelo é usado tanto na indexação quanto na query —
# MAGIC é fundamental que sejam o mesmo modelo pra manter consistência do espaço vetorial.

# COMMAND ----------

# modelo de embedding via Foundation Model APIs
embeddings = DatabricksEmbeddings(
    endpoint=embedding_endpoint,
    # dimensão 1024 para BGE Large EN
)

# teste rápido de embedding
teste_embed = embeddings.embed_query("teste de embedding")
print(f"Dimensão do embedding: {len(teste_embed)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Componente 2 — Vector Search Retriever
# MAGIC
# MAGIC O índice do Vector Search é criado a partir de uma tabela Delta com os documentos
# MAGIC da base de conhecimento. O índice é atualizado automaticamente quando a tabela Delta
# MAGIC muda (sync incremental configurado no endpoint).
# MAGIC
# MAGIC k=5 → recupera os 5 documentos mais similares à query.
# MAGIC Filtros adicionais podem ser aplicados por metadata (ex: categoria, data).

# COMMAND ----------

vsc = VectorSearchClient(
    workspace_url=os.environ["DATABRICKS_HOST"],
    personal_access_token=os.environ["DATABRICKS_TOKEN"],
)

# conecta ao índice existente
vector_store = DatabricksVectorSearch(
    endpoint_name=vs_endpoint,
    index_name=vs_index,
    embedding=embeddings,
    text_column="conteudo",       # campo da tabela Delta com o texto dos docs
    columns=["titulo", "categoria", "dt_atualizacao"],  # metadados retornados
)

retriever = vector_store.as_retriever(
    search_type="similarity",
    search_kwargs={
        "k": k_docs,
        # filtros de metadata (opcional) — ex: só documentos ativos
        # "filter": {"status": "ativo"},
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Componente 3 — LLM (DBRX Instruct)
# MAGIC
# MAGIC DBRX é o LLM aberto da Databricks, otimizado para tarefas de instrução.
# MAGIC O endpoint Foundation Model API funciona sem precisar provisionar GPU manualmente.
# MAGIC
# MAGIC Parâmetros de geração:
# MAGIC - temperature=0.1: baixo para respostas mais determinísticas (factual QA)
# MAGIC - max_tokens=1024: limite razoável para respostas completas sem custo excessivo

# COMMAND ----------

llm = ChatDatabricks(
    endpoint=llm_endpoint,
    temperature=0.1,
    max_tokens=1024,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Componente 4 — Prompt Template
# MAGIC
# MAGIC O template instrui o LLM a:
# MAGIC 1. Responder APENAS com base nos documentos recuperados (evita alucinação)
# MAGIC 2. Citar a fonte quando possível
# MAGIC 3. Dizer "não sei" quando a resposta não está no contexto
# MAGIC
# MAGIC TODO: melhorar o prompt adicionando exemplos de respostas bem formatadas
# MAGIC (few-shot dentro do template) para casos onde o usuário pede resumos tabulares.

# COMMAND ----------

template_rag = """Você é um assistente especializado em dados da empresa.
Use APENAS as informações dos documentos abaixo para responder.
Se a resposta não estiver nos documentos, diga "Não encontrei essa informação na base de conhecimento."

Documentos recuperados:
{context}

Pergunta: {question}

Resposta (em português, clara e objetiva):"""

prompt_rag = PromptTemplate(
    template=template_rag,
    input_variables=["context", "question"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montagem da Chain RAG

# COMMAND ----------

chain_rag = RetrievalQA.from_chain_type(
    llm=llm,
    chain_type="stuff",    # stuff = coloca todos os docs no prompt de uma vez
                           # alternativas: map_reduce, refine (para muitos docs)
    retriever=retriever,
    chain_type_kwargs={
        "prompt": prompt_rag,
        "verbose": False,
    },
    return_source_documents=True,    # retorna os docs usados na resposta
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo de Query

# COMMAND ----------

def pergunta_rag(query: str) -> dict:
    """
    Faz uma pergunta ao pipeline RAG e exibe a resposta + fontes.
    """
    print(f"\nPergunta: {query}")
    print("-" * 60)

    resultado = chain_rag.invoke({"query": query})

    print(f"Resposta:\n{resultado['result']}")
    print("\nFontes utilizadas:")
    for i, doc in enumerate(resultado["source_documents"], 1):
        titulo = doc.metadata.get("titulo", "sem título")
        cat    = doc.metadata.get("categoria", "")
        print(f"  [{i}] {titulo} ({cat})")

    return resultado


# exemplos de queries
pergunta_rag("Qual foi a receita total do mês de março?")
pergunta_rag("Quais clientes estão em risco de churn segundo o modelo RFM?")
pergunta_rag("Como funciona o processo de aprovação de transações acima de R$10.000?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log da chain no MLflow

# COMMAND ----------

mlflow.set_experiment("/leandro/rag_pipeline")

with mlflow.start_run(run_name="rag_chain_v1"):
    mlflow.log_params({
        "llm_endpoint":       llm_endpoint,
        "embedding_endpoint": embedding_endpoint,
        "vs_index":           vs_index,
        "k_docs":             k_docs,
        "chain_type":         "stuff",
        "temperature":        0.1,
    })

    # loga a chain como modelo MLflow para servir via Model Serving
    mlflow.langchain.log_model(
        chain_rag,
        artifact_path="rag_chain",
        registered_model_name="rag_pipeline_knowledge_base",
    )

    print("Chain RAG registrada no MLflow Model Registry")
