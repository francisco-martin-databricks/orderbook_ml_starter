# Databricks notebook source
# MAGIC %md
# MAGIC # 4 · Score with `@champion`
# MAGIC Scores one day of `minute_features` with whatever version is `@champion` right now and writes the result to the
# MAGIC `predictions` table, with the model version and the actual return on every row. That gives two things:
# MAGIC - **Performance over time on real predictions:** MAE per day and per model version is one query on this table.
# MAGIC - **Drift:** a data profiling monitor (formerly Lakehouse Monitoring) on the table, created here the first time.
# MAGIC
# MAGIC Rerunning a day with the same version replaces its rows. A day scored by two versions keeps both, for comparison.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow>=3"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import datetime as dt

import mlflow
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("score_date", "")  # YYYY-MM-DD; empty = yesterday (UTC)
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
day = dbutils.widgets.get("score_date") or str(dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))
MODEL, TABLE, LABEL = f"{catalog}.{schema}.next_5min_return_lr", f"{catalog}.{schema}.predictions", "next_5min_return_bps"
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

version = mlflow.MlflowClient().get_model_version_by_alias(MODEL, "champion").version
model = mlflow.sklearn.load_model(f"models:/{MODEL}@champion")
df = spark.sql(f"SELECT * FROM {catalog}.{schema}.minute_features WHERE to_date(ts) = '{day}'").toPandas()
df["prediction_bps"] = model.predict(df[model.feature_names_in_])
out = (spark.createDataFrame(df).withColumnRenamed(LABEL, "actual_bps")
       .withColumn("model_version", F.lit(str(version))).withColumn("scored_at", F.current_timestamp()))
if spark.catalog.tableExists(TABLE):
    spark.sql(f"DELETE FROM {TABLE} WHERE to_date(ts) = '{day}' AND model_version = '{version}'")
out.write.mode("append").saveAsTable(TABLE)
print(f"{day}: {len(df):,} predictions from v{version} written to {TABLE}")

# COMMAND ----------

w = WorkspaceClient()
try:
    w.quality_monitors.get(TABLE)
    w.quality_monitors.run_refresh(TABLE)
except Exception:  # first run: create the monitor (its first refresh starts by itself)
    w.quality_monitors.create(
        table_name=TABLE, output_schema_name=f"{catalog}.{schema}",
        assets_dir=f"/Workspace/Users/{w.current_user.me().user_name}/orderbook-ml-starter/monitoring",
        inference_log=MonitorInferenceLog(problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_REGRESSION,
                                          prediction_col="prediction_bps", label_col="actual_bps",
                                          model_id_col="model_version", timestamp_col="ts", granularities=["1 day"]))
print(f"Data profiling monitor on {TABLE}: refresh started")
