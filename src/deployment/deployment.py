# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Deployment (deployment job, task 3)
# MAGIC Moves `@champion` to the approved version. Rolling back means pointing the alias at an older version.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow>=3"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from mlflow import MlflowClient

dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")
MODEL, VERSION = dbutils.widgets.get("model_name"), dbutils.widgets.get("model_version")
MlflowClient(registry_uri="databricks-uc").set_registered_model_alias(MODEL, "champion", VERSION)
print(f"@champion → v{VERSION}")
