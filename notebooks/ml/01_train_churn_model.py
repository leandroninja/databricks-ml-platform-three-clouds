# Databricks notebook source
# MAGIC %md
# MAGIC # ML — Treino de Modelo de Churn (XGBoost + Hyperopt + MLflow)
# MAGIC
# MAGIC Treina um modelo de predição de churn de clientes usando XGBoost com
# MAGIC busca automática de hiperparâmetros via Hyperopt (TPE).
# MAGIC
# MAGIC **Por que XGBoost?**
# MAGIC - Melhor desempenho empírico em dados tabulares com features mistas
# MAGIC - Robusto a outliers e valores nulos
# MAGIC - Treino rápido comparado a LightGBM neste dataset (testamos ambos)
# MAGIC - Suporte nativo a feature importance para explicabilidade
# MAGIC
# MAGIC Todo o ciclo de treino é rastreado no MLflow:
# MAGIC - Run pai: experimento completo
# MAGIC - Runs filhos: cada trial do Hyperopt
# MAGIC - Melhor modelo: registrado no Model Registry com assinatura

# COMMAND ----------

# MAGIC %pip install xgboost hyperopt shap

# COMMAND ----------

import mlflow
import mlflow.xgboost
from mlflow.models.signature import infer_signature

import xgboost as xgb
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll import scope

import pandas as pd
import numpy as np
from pyspark.sql import functions as F

from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score,
    average_precision_score
)
from sklearn.preprocessing import LabelEncoder

import shap
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("catalog",          "lakehouse_prod",         "Unity Catalog")
dbutils.widgets.text("schema_gold",      "gold",                   "Schema Gold")
dbutils.widgets.text("schema_ml",        "ml",                     "Schema ML")
dbutils.widgets.text("experimento_nome", "/leandro/churn_xgboost", "MLflow Experiment")
dbutils.widgets.text("max_trials",       "50",                     "Max trials Hyperopt")
dbutils.widgets.text("modelo_nome",      "churn_predictor",        "Nome modelo Registry")

catalog         = dbutils.widgets.get("catalog")
schema_gold     = dbutils.widgets.get("schema_gold")
schema_ml       = dbutils.widgets.get("schema_ml")
experimento     = dbutils.widgets.get("experimento_nome")
max_trials      = int(dbutils.widgets.get("max_trials"))
modelo_nome     = dbutils.widgets.get("modelo_nome")

mlflow.set_experiment(experimento)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos dados de treino
# MAGIC
# MAGIC Features vêm da tabela RFM (Gold) + dados de transação dos últimos 6 meses.
# MAGIC Label: cliente é considerado "churn" se não fez nenhuma transação nos últimos 30 dias
# MAGIC mas tinha histórico nos 6 meses anteriores.

# COMMAND ----------

df_rfm = spark.table(f"{catalog}.{schema_gold}.rfm_clientes")

# define churn: recency > 30 dias (não comprou no último mês)
df_features = (
    df_rfm
    .withColumn("churn",
        F.when(F.col("recency_dias") > 30, F.lit(1))
         .otherwise(F.lit(0))
    )
    .select(
        "id_cliente",
        "recency_dias",
        "frequency",
        "monetary",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "segmento",
        "churn",
    )
)

# converte para pandas (dataset de ML é menor — cabe na memória do driver)
df_pd = df_features.toPandas()

print(f"Dataset: {len(df_pd):,} clientes")
print(f"Taxa de churn: {df_pd['churn'].mean():.2%}")

# COMMAND ----------

# encoding do campo categórico segmento
le = LabelEncoder()
df_pd["segmento_enc"] = le.fit_transform(df_pd["segmento"])

feature_cols = [
    "recency_dias", "frequency", "monetary",
    "r_score", "f_score", "m_score", "rfm_score",
    "segmento_enc"
]

X = df_pd[feature_cols]
y = df_pd["churn"]

# split estratificado para manter proporção de churn nos sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

X_train, X_val, y_train, y_val = train_test_split(
    X_train, y_train, test_size=0.15, random_state=42, stratify=y_train
)

print(f"Train: {len(X_train):,} | Val: {len(X_val):,} | Test: {len(X_test):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Espaço de busca do Hyperopt
# MAGIC
# MAGIC TPE (Tree-structured Parzen Estimator) é mais eficiente que grid search —
# MAGIC aprende quais regiões do espaço tendem a ter melhores resultados e foca nelas.

# COMMAND ----------

espaco = {
    "max_depth":        scope.int(hp.quniform("max_depth", 3, 12, 1)),
    "min_child_weight": scope.int(hp.quniform("min_child_weight", 1, 10, 1)),
    "subsample":        hp.uniform("subsample", 0.6, 1.0),
    "colsample_bytree": hp.uniform("colsample_bytree", 0.6, 1.0),
    "learning_rate":    hp.loguniform("learning_rate", np.log(0.005), np.log(0.3)),
    "n_estimators":     scope.int(hp.quniform("n_estimators", 100, 800, 50)),
    "reg_alpha":        hp.loguniform("reg_alpha", np.log(1e-4), np.log(10)),
    "reg_lambda":       hp.loguniform("reg_lambda", np.log(1e-4), np.log(10)),
    "gamma":            hp.uniform("gamma", 0, 5),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função objetivo — cada trial do Hyperopt cria um run filho no MLflow

# COMMAND ----------

def objetivo(params):
    """
    Treina um XGBoost com os params do Hyperopt e loga métricas no MLflow.
    Retorna loss (1 - AUC) para minimização.
    """
    with mlflow.start_run(nested=True):
        modelo = xgb.XGBClassifier(
            **params,
            objective="binary:logistic",
            eval_metric="auc",
            use_label_encoder=False,
            tree_method="hist",     # mais rápido que exact em datasets grandes
            random_state=42,
            n_jobs=-1,
        )

        modelo.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            early_stopping_rounds=20,
            verbose=False,
        )

        y_pred_proba = modelo.predict_proba(X_val)[:, 1]
        y_pred       = (y_pred_proba >= 0.5).astype(int)

        auc = roc_auc_score(y_val, y_pred_proba)
        f1  = f1_score(y_val, y_pred, zero_division=0)
        pr  = precision_score(y_val, y_pred, zero_division=0)
        rc  = recall_score(y_val, y_pred, zero_division=0)
        ap  = average_precision_score(y_val, y_pred_proba)

        mlflow.log_params(params)
        mlflow.log_metrics({
            "val_auc":       auc,
            "val_f1":        f1,
            "val_precision": pr,
            "val_recall":    rc,
            "val_avg_precision": ap,
        })

        return {"loss": -auc, "status": STATUS_OK, "model": modelo}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do Hyperopt — Run pai no MLflow

# COMMAND ----------

with mlflow.start_run(run_name="churn_xgboost_hyperopt") as run_pai:
    mlflow.log_param("max_trials", max_trials)
    mlflow.log_param("dataset_size", len(df_pd))
    mlflow.log_param("churn_rate", round(df_pd["churn"].mean(), 4))
    mlflow.log_param("features", feature_cols)

    trials = Trials()

    melhor = fmin(
        fn=objetivo,
        space=espaco,
        algo=tpe.suggest,
        max_evals=max_trials,
        trials=trials,
        rstate=np.random.default_rng(42),
    )

    print(f"\nMelhores hiperparâmetros: {melhor}")

    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Treino final com os melhores hiperparâmetros (no set completo train+val)

    # COMMAND ----------

    X_train_full = pd.concat([X_train, X_val])
    y_train_full = pd.concat([y_train, y_val])

    # corrige tipos — Hyperopt retorna float pra campos int
    melhor["max_depth"]        = int(melhor["max_depth"])
    melhor["min_child_weight"] = int(melhor["min_child_weight"])
    melhor["n_estimators"]     = int(melhor["n_estimators"])

    modelo_final = xgb.XGBClassifier(
        **melhor,
        objective="binary:logistic",
        eval_metric="auc",
        use_label_encoder=False,
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )

    modelo_final.fit(X_train_full, y_train_full, verbose=False)

    # avalia no test set (dados nunca vistos)
    y_pred_test_proba = modelo_final.predict_proba(X_test)[:, 1]
    y_pred_test       = (y_pred_test_proba >= 0.5).astype(int)

    auc_test = roc_auc_score(y_test, y_pred_test_proba)
    f1_test  = f1_score(y_test, y_pred_test, zero_division=0)
    pr_test  = precision_score(y_test, y_pred_test, zero_division=0)
    rc_test  = recall_score(y_test, y_pred_test, zero_division=0)

    mlflow.log_metrics({
        "test_auc":       auc_test,
        "test_f1":        f1_test,
        "test_precision": pr_test,
        "test_recall":    rc_test,
    })

    print(f"\nTest AUC: {auc_test:.4f} | F1: {f1_test:.4f} | Precision: {pr_test:.4f} | Recall: {rc_test:.4f}")

    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Assinatura do modelo e registro no MLflow Model Registry

    # COMMAND ----------

    # assinatura: define o schema de input e output esperados
    assinatura = infer_signature(X_test, y_pred_test_proba)

    # loga e registra no Model Registry
    mlflow.xgboost.log_model(
        modelo_final,
        artifact_path="model",
        signature=assinatura,
        registered_model_name=modelo_nome,
        input_example=X_test.head(5),
    )

    # feature importance via SHAP (melhor que o built-in do XGBoost)
    explainer   = shap.TreeExplainer(modelo_final)
    shap_values = explainer.shap_values(X_test)
    shap_imp    = pd.DataFrame({
        "feature":    feature_cols,
        "importance": np.abs(shap_values).mean(axis=0)
    }).sort_values("importance", ascending=False)

    print("\nFeature Importance (SHAP):")
    print(shap_imp.to_string(index=False))

    mlflow.log_table(shap_imp, "shap_importance.json")

    run_id = run_pai.info.run_id
    print(f"\nRun ID: {run_id}")
    print(f"Modelo registrado: {modelo_nome}")
