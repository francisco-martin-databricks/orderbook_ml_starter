# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Setup (first task of `train_model`, safe to rerun)
# MAGIC Creates the `minute_features` view on bronze and the UC model, and links the model to the
# MAGIC `model_deployment` job, so that every new version starts it.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow>=3"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from mlflow import MlflowClient

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("deployment_job_id", "")
catalog, schema, job_id = (dbutils.widgets.get(k) for k in ["catalog", "schema", "deployment_job_id"])
MODEL = f"{catalog}.{schema}.next_5min_return_lr"

spark.sql(f"USE {catalog}.{schema}")
spark.sql(open("minute_features.sql").read())

client = MlflowClient(registry_uri="databricks-uc")
try:
    client.get_registered_model(MODEL)
except Exception:
    client.create_registered_model(MODEL, description="Next 5-minute return (bps) from spread and order imbalance. Retrained daily.")
client.update_registered_model(MODEL, deployment_job_id=job_id)
print(f"View {catalog}.{schema}.minute_features ready; model {MODEL} linked to deployment job {job_id}")
