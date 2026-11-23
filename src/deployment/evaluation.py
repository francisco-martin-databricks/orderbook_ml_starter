# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Evaluation (deployment job, task 1)
# MAGIC Runs for every new version. Scores the candidate and `@champion` on the candidate's holdout (the last 6 hours of its
# MAGIC training day, which neither model trained on) and applies one rule:
# MAGIC **the MAE is at most 5% worse than the champion's, and no coefficient changed sign.**
# MAGIC
# MAGIC | `approval_mode` | Rule passes | Rule fails |
# MAGIC |---|---|---|
# MAGIC | `auto` | approved here, promoted with no human | waits at `Approval_Check` for **Approve** |
# MAGIC | `human` | waits at `Approval_Check` for **Approve** | waits at `Approval_Check` for **Approve** |
# MAGIC
# MAGIC Either way, the result is written on the version as the `evaluation` tag, for whoever approves.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow>=3"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from sklearn.metrics import mean_absolute_error

dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")
dbutils.widgets.text("approval_mode", "auto")  # auto | human
MODEL, VERSION, MODE = (dbutils.widgets.get(k) for k in ["model_name", "model_version", "approval_mode"])
LABEL = "next_5min_return_bps"
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
cand = client.get_run(client.get_model_version(MODEL, VERSION).run_id).data
champion = client.get_registered_model(MODEL).aliases.get("champion")

# COMMAND ----------

if champion is None:
    passes, result = True, "first version, no @champion yet"
else:
    champ = client.get_run(client.get_model_version(MODEL, champion).run_id).data
    holdout = spark.sql(f"""SELECT * FROM {MODEL.rsplit('.', 1)[0]}.minute_features WHERE to_date(ts) = '{cand.params['train_date']}'
                            AND hour(ts) >= 18 AND {LABEL} IS NOT NULL""").toPandas()
    models = {v: mlflow.sklearn.load_model(f"models:/{MODEL}/{v}") for v in (VERSION, champion)}
    mae = {v: mean_absolute_error(holdout[LABEL], m.predict(holdout[m.feature_names_in_])) for v, m in models.items()}
    flips = [f"{k[5:]} {champ.metrics[k]:+.2f} → {c:+.2f}" for k, c in cand.metrics.items()
             if k.startswith("coef.") and c * champ.metrics.get(k, c) < 0]
    passes = mae[VERSION] <= 1.05 * mae[champion] and not flips
    result = f"MAE {mae[VERSION]:.3f} vs champion v{champion} {mae[champion]:.3f}" + (f"; sign flip: {', '.join(flips)}" if flips else "")

approved = MODE == "auto" and passes
verdict = "approved automatically" if approved else f"rule {'passed' if passes else 'failed'}, waiting for Approve"
client.set_model_version_tag(MODEL, VERSION, "evaluation", f"{result}: {verdict}"[:250])
if approved:
    client.set_model_version_tag(MODEL, VERSION, "Approval_Check", "Approved")
print(f"v{VERSION} ({MODE}): {result}: {verdict}")
