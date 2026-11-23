# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Approval_Check (deployment job, task 2)
# MAGIC Passes only when the version has the tag `Approval_Check = Approved`. Evaluation sets it when `approval_mode = auto`
# MAGIC and the rule passes. Otherwise, clicking **Approve** on the model version page sets it and repairs this run.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow>=3"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from mlflow import MlflowClient

dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")
MODEL, VERSION = dbutils.widgets.get("model_name"), dbutils.widgets.get("model_version")
tags = MlflowClient(registry_uri="databricks-uc").get_model_version(MODEL, VERSION).tags
if tags.get("Approval_Check", "").lower() != "approved":
    raise Exception(f"v{VERSION} is waiting for approval ({tags.get('evaluation')}). Click Approve on the model version page.")
print(f"v{VERSION} approved")
