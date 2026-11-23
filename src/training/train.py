# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Train
# MAGIC Trains a linear regression on one day of `minute_features` (the first 18 hours), measures it on the last 6 hours,
# MAGIC logs every coefficient and the error, and registers a new version of the UC model. The new version starts the
# MAGIC `model_deployment` job by itself; promotion is not decided here.
# MAGIC
# MAGIC The features are every column of the view except the keys and the label, so changing the view changes the model.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow>=3"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import datetime as dt

import mlflow
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("experiment_id", "")
dbutils.widgets.text("train_date", "")  # YYYY-MM-DD; empty = yesterday (UTC)
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
day = dbutils.widgets.get("train_date") or str(dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))
MODEL, LABEL = f"{catalog}.{schema}.next_5min_return_lr", "next_5min_return_bps"
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_id=dbutils.widgets.get("experiment_id"))

# COMMAND ----------

df = spark.sql(f"SELECT * FROM {catalog}.{schema}.minute_features WHERE to_date(ts) = '{day}' AND {LABEL} IS NOT NULL").toPandas()
FEATURES = [c for c in df.columns if c not in ("exchange", "ticker", "ts", LABEL)]
train, holdout = df[df.ts.dt.hour < 18], df[df.ts.dt.hour >= 18]
assert len(train) > 100 and len(holdout) > 100, f"not enough data on {day}: {len(train)} train and {len(holdout)} holdout rows"
model = LinearRegression().fit(train[FEATURES], train[LABEL])

# COMMAND ----------

with mlflow.start_run(run_name=f"train_{day}"):
    mlflow.set_tag("run_type", "train")
    mlflow.log_params({"train_date": day, "rows_train": len(train), "rows_holdout": len(holdout)})
    info = mlflow.sklearn.log_model(model, name="model", input_example=train[FEATURES].head(5), registered_model_name=MODEL)
    metrics = {**{f"coef.{f}": float(c) for f, c in zip(FEATURES, model.coef_)}, "intercept": float(model.intercept_),
               "holdout_mae": mean_absolute_error(holdout[LABEL], model.predict(holdout[FEATURES])),
               "train_end_ts": train.ts.max().timestamp()}
    mlflow.log_metrics(metrics, model_id=info.model_id)  # on the run and on the model version page
mlflow.MlflowClient().set_model_version_tag(MODEL, info.registered_model_version, "train_date", day)
print(f"{MODEL} v{info.registered_model_version} ({day}): " + ", ".join(f"{k} {v:.3f}" for k, v in metrics.items() if k != "train_end_ts"))
