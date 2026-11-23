# AGENT.md: how to install, run and change `orderbook-ml-starter`

**Date:** 2026-11-22
**Audience:** a coding agent (Claude Code, Cursor, Codex, ...) helping someone deploy this project in their own Databricks workspace.
**Verified:** end to end on 2026-11-22 with Databricks CLI v1.21.0 on an AWS workspace with Unity Catalog. Everything under "Known behaviour" was observed, not assumed.

This is a small Declarative Automation Bundle (formerly Databricks Asset Bundle) on **synthetic data**. It shows five pieces end to end. Keep every piece in its simplest working form. Do not add features, abstractions or error handling unless the user asks.

| # | Piece | File(s) | Runs on |
|---|-------|---------|---------|
| 0 | Exchange feed: writes one JSON file per minute (3 corridors, 20 book levels a side) into a UC volume that stands in for S3 | `src/feed/exchange_feed.py`, `resources/exchange_feed.job.yml` | Serverless continuous job, paused by default |
| 1 | Bronze pipeline: Auto Loader appends every file to `orderbook_bronze` (top 5 levels as arrays, full record as `raw` VARIANT) | `src/orderbook_bronze/transformations/orderbook_bronze.sql`, `resources/orderbook_bronze.pipeline.yml` | **Classic** single node, continuous, Core edition |
| 1 | Features view: 2 drivers + the 5-minute label, computed when read | `src/training/minute_features.sql` | Nothing runs |
| 2 | Training: fits a linear regression on one day, logs every coefficient, registers a UC model version | `src/training/setup.py`, `train.py`, `resources/train_model.job.yml` | Serverless job |
| 3 | Deployment job (MLflow 3, Public Preview): Evaluation → `Approval_Check` → Deployment moves `@champion` | `src/deployment/*.py`, `resources/model_deployment.job.yml` | Serverless job, started by every new model version |
| 4 | Scoring: `@champion` scores one day into `predictions`; a data profiling monitor watches it | `src/scoring/score.py`, `resources/score_predictions.job.yml` | Serverless job |

Human-facing steps: `README.md`.

---

## 1. Ask the user first (do not guess, do not deploy before you have answers)

The goal is **isolation**: everything this project creates must land in a catalog and schema chosen for it, in the workspace the user names, and nowhere else. Ask these in one message and wait:

1. **Workspace and CLI profile.** Run `databricks auth profiles` and show the list. Never pick a profile yourself. If none fits, have the user run `databricks auth login --host <workspace-url> --profile <name>`.
2. **Cloud.** The project is written for AWS (`aws_attributes`, node `r5d.xlarge`). On Azure or GCP, remove the `aws_attributes` block in `resources/orderbook_bronze.pipeline.yml` and ask for a 4 vCPU / 32 GB node type.
3. **Catalog.** An existing catalog where they have `USE CATALOG` and `CREATE SCHEMA`. Do not use a shared or production catalog unless the user says so explicitly.
4. **Schema.** A **new** schema name used only by this demo (default `orderbook_ml_starter`). The bundle creates it. Before deploying, check that it does not exist yet: `databricks schemas get <catalog>.<schema> --profile <p>` should return `RESOURCE_DOES_NOT_EXIST`. If it exists, ask whether it is a previous deployment of this project or something else; never deploy into someone else's schema.
5. **Node type** for the pipeline (default `r5d.xlarge`, 4 vCPU / 32 GB, the documented driver minimum).
6. **Approval default:** `auto` (a version that passes the rule is promoted with no human) or `human` (every version waits for **Approve**).
7. **Is another copy of this bundle already deployed in the same workspace by the same user?** Several paths are fixed to the folder name `orderbook-ml-starter` (list in section 2). A second copy must change them, or it will overwrite the first.
8. **Cost acknowledgement.** While the demo is live, the pipeline keeps one classic on-demand node running around the clock, plus a serverless feed job. Both run until stopped (section 5).

### Where the answers go

Write them as **defaults in `databricks.yml`**, not as `--var` flags. `scripts/live.sh` and `scripts/reset_demo.sh` call `databricks bundle ...` without `--var`, and `reset_demo.sh` redeploys. Values passed only on the command line are lost on the next deploy, and the bundle would then point at the default catalog.

```yaml
# databricks.yml
variables:
  catalog:
    default: <catalog>          # answer 3
  schema:
    default: <schema>           # answer 4
  node_type:
    default: <node type>        # answer 5
  approval_mode:
    default: auto               # answer 6
targets:
  dev:
    default: true
    workspace:
      host: <workspace-url>     # answer 1 (no profile here, see below)
```

**Do not add `profile` to the target.** Pass `--profile <profile>` on every CLI command instead. A profile in `databricks.yml` breaks deploying from the workspace web terminal, which authenticates from environment variables and doesn't support profiles. Locally, when two profiles share the host, a command without `--profile` stops with "multiple profiles matched"; that is expected.

Then export the profile for the scripts, which default to the author's profile: `export PROFILE=<profile>` (or prefix each call: `PROFILE=<profile> scripts/live.sh status`). The scripts set it only when `DATABRICKS_HOST` is unset, so in the web terminal they use the terminal's own login.

Do **not** add `mode: development` to the target: development mode deploys pipelines as triggered, not continuous.

---

## 2. What gets created, and where (for isolation checks)

| Object | Name | Where |
|--------|------|-------|
| Schema | `<schema>` | `<catalog>` |
| Volume (stands in for S3) | `landing`, files under `landing/exchange_1/<YYYY-MM-DD>/<HHMM>.json` | `<catalog>.<schema>` |
| Streaming table | `orderbook_bronze` | `<catalog>.<schema>` |
| View | `minute_features` | `<catalog>.<schema>` |
| Table | `predictions`, plus the monitor's `predictions_profile_metrics` and `predictions_drift_metrics` | `<catalog>.<schema>` |
| UC model | `next_5min_return_lr` | `<catalog>.<schema>` |
| Pipeline and 4 jobs | `[orderbook-ml-starter] ...` (prefix is `bundle.name`) | Workspace |
| Bundle files | `/Workspace/Users/<user>/orderbook-ml-starter/` | Workspace, hard-coded in `databricks.yml` `root_path` |
| MLflow experiment | `/Workspace/Users/<user>/orderbook-ml-starter/experiment` | Hard-coded in `resources/schema.yml` |
| Monitor dashboard assets | `/Workspace/Users/<user>/orderbook-ml-starter/monitoring` | Hard-coded in `src/scoring/score.py` |

All data objects live in the one schema, so dropping the schema removes all the data. For a **second copy in the same workspace and user**, change these four together: `bundle.name`, `root_path` in `databricks.yml`, the experiment `name` in `resources/schema.yml`, and `assets_dir` in `src/scoring/score.py`.

No credentials belong in this folder. Authentication comes from the CLI profile only.

---

## 3. Prerequisites (check them, don't assume)

- Databricks CLI: `databricks -v`. Verified on v1.21.0; older versions untested.
- Unity Catalog workspace with **serverless jobs and notebooks** enabled (every job here is serverless; only the pipeline is classic).
- Permission to create **classic clusters for pipelines** (directly or through a policy), jobs, and MLflow experiments in the user's home folder.
- **MLflow 3 deployment jobs** available (Public Preview).
- The notebooks run `%pip install -U "mlflow>=3"`, so serverless needs PyPI access.
- A SQL warehouse, only for `reset_demo.sh` (it empties `predictions`) and for the SQL checks below. `databricks experimental aitools tools get-default-warehouse --profile <p>` finds one.

---

## 4. Install and run (in this order)

| Step | Command | What to expect |
|------|---------|----------------|
| 1. Validate | `databricks bundle validate` | A "Single node cluster is not correctly configured" warning on every command. It is generic and safe to ignore; the pipeline sets single node itself. |
| 2. Deploy | `databricks bundle deploy` | Creates the schema, volume, experiment, pipeline and 4 jobs. **The first deploy starts the pipeline.** Its classic cluster takes about 7 to 8.5 minutes to start. |
| 3. Start the feed | `scripts/live.sh start` | Unpauses the feed and starts the pipeline if it is idle. On the first start the feed writes 3 days of history from midnight UTC plus today so far (about 1 minute), then one file a minute. |
| 4. Wait for bronze | SQL check A below | `lag_s` under about 120 seconds. Each live file lands in bronze about 4 to 5 seconds after its minute. |
| 5. Train 3 days, oldest first | `databricks bundle run train_model --params train_date=<day>` for each of the first three folders under `landing/exchange_1/` | About 2 minutes per run. Each new version starts `model_deployment` by itself (queued, one at a time, about 2 to 3 minutes each). |
| 6. Check the opening state | The model page in Catalog Explorer, or `databricks api get "/api/2.1/unity-catalog/models/<c>.<s>.next_5min_return_lr?include_aliases=true"` | `@champion` = v2. v3 waits at `Approval_Check`, with an `evaluation` tag naming the sign flip of `spread_bps`. |
| 7. Score day 3 | `databricks bundle run score_predictions --params score_date=<day 3>` | `predictions` gets 4,320 rows tagged `model_version = 2`. The first run creates the monitor and its dashboard. |
| 8. Stop | `scripts/live.sh stop`, then `scripts/live.sh status` | `orderbook_bronze: IDLE` and `exchange_feed: PAUSED`. Also confirm no active feed run (section 6, item 2). |

Only train **complete** days: training needs more than 100 rows in both the first 18 hours and the last 6 hours (UTC) of the day.

### Deploying from the workspace instead of a laptop

If the user wants the files visible and deployable in the workspace, copy the source to a regular folder (not the bundle's `root_path`) and deploy from the web terminal:

```bash
# from the project folder on the laptop
databricks sync --include "src/**" --include "resources/**" --include "scripts/**" --include "*.md" --include "databricks.yml" \
  --exclude ".databricks/**" . /Workspace/Users/<user>/orderbook-ml-starter-source --profile <profile>
# then, in the workspace web terminal (admin-enabled, classic compute on DBR 15.0+)
cd /Workspace/Users/<user>/orderbook-ml-starter-source && databricks bundle deploy && bash scripts/live.sh status
```

- Check the destination doesn't exist first (`databricks workspace get-status <path>` returns "doesn't exist"); never sync over someone else's folder.
- A deploy from the workspace uses the same bundle identity (name, target, user) and therefore the same resources and state as a laptop deploy.
- The workspace bundle editor's **Deploy** button needs a Git folder; a regular folder deploys through the web terminal.
- The sync is one way; re-syncing overwrites edits made in the workspace.

### Demo operations

- **Approve** a waiting version: the **Approve** button on the model version page sets `Approval_Check = Approved` and repairs the run. Prefer the button. The CLI equivalent needs the run's parameters and, if the run was repaired before, the latest repair ID:

  ```bash
  databricks api post /api/2.0/mlflow/unity-catalog/model-versions/set-tag --profile $PROFILE \
    --json '{"name":"<catalog>.<schema>.next_5min_return_lr","version":"<v>","key":"Approval_Check","value":"Approved"}'
  RID=$(databricks api get "/api/2.2/jobs/runs/get?run_id=<run_id>&include_history=true" --profile $PROFILE \
    | python3 -c "import json,sys; h=[x['id'] for x in json.load(sys.stdin).get('repair_history',[]) if x.get('type')=='REPAIR']; print(h[-1] if h else '')")
  databricks jobs repair-run --profile $PROFILE --json "{\"run_id\": <run_id>, ${RID:+\"latest_repair_id\": $RID,} \"rerun_all_failed_tasks\": true,
    \"job_parameters\": {\"model_name\": \"<catalog>.<schema>.next_5min_return_lr\", \"model_version\": \"<v>\", \"approval_mode\": \"auto\"}}"
  ```

  A repair **without** `job_parameters` reruns with the job default `model_version = ""` and fails with `For input string: ""`. That is deliberate: it can no longer promote the wrong version.
- **Approval switch:** change `approval_mode` in the job's parameters in the Jobs UI, or `databricks bundle deploy --var approval_mode=human`. The next version follows the new mode. Deploy again without the flag to go back to the default.
- **Rehearse, then reset:** `scripts/reset_demo.sh` (about 8 minutes; its header says 15). It redeploys, deletes the model (every version) and the experiment's runs, empties `predictions`, trains the first three days again and scores day 3. Bronze and the landing files are kept.

---

## 5. Cost guard

The feed and the pipeline run until they are stopped. Run `scripts/live.sh stop` after every setup, rehearsal and demo, and check with `scripts/live.sh status` plus the active-run check below. Never leave the user's workspace with the pipeline `RUNNING` unless they ask for it.

---

## 6. Known behaviour (observed on 2026-11-22)

1. **The first deploy starts the pipeline; a redeploy that changes only SQL does not.** Changing pipeline settings starts it again.
2. **`bundle deploy` sets the feed job back to `PAUSED`** (the bundle declares it paused), but a feed run already in progress **keeps writing**, while `live.sh status` reports `PAUSED`. To really stop the feed, use `scripts/live.sh stop` (pause plus cancel). Check for an active run:
   `databricks jobs list-runs --job-id <exchange_feed id> --active-only --profile $PROFILE`
3. **If you change the bronze columns** (the SQL in `transformations/`), run a full refresh once: `databricks pipelines start-update <pipeline id> --full-refresh --profile $PROFILE`.
4. **If you change the landing file format**, delete the old files first (`databricks fs rm -r dbfs:/Volumes/<catalog>/<schema>/landing/exchange_1 --profile $PROFILE`). The feed resumes from the last file it finds and crashes on a file in another format (`AttributeError: 'Pandas' object has no attribute 'bids'`).
5. A waiting `Approval_Check` ends as `INTERNAL_ERROR` and is retried once despite `max_retries: 0`. Harmless.
6. A second `score_predictions` run while the monitor is still refreshing queues another refresh (`PENDING`); it does not fail.
7. **`reset_demo.sh` stops on the first error** (`set -euo pipefail`), including a transient network error while `bundle run` waits on a run (seen once: `unexpected EOF`). The workspace run itself carries on. To recover: wait until `model_deployment` has no active run (`databricks jobs list-runs --job-id <model_deployment id> --active-only`), then run the script again from the start.
8. `databricks bundle summary -o json` gives every resource ID; the scripts use it.

---

## 7. SQL checks

Replace `<c>.<s>` with the catalog and schema. Run them with `databricks experimental aitools tools query "<sql>" --profile $PROFILE`.

```sql
-- A. Bronze is current (lag in seconds)
SELECT count(*) AS n, max(ts) AS max_ts, timestampdiff(SECOND, max(ts), current_timestamp()) AS lag_s FROM <c>.<s>.orderbook_bronze;

-- B. One row per ticker and minute, 5 parsed levels, 20 in raw, no gaps
SELECT to_date(ts) AS d, count(*) AS n, count(DISTINCT ticker, ts) AS uniq, min(size(bids)) AS levels,
       min(size(raw:bids::array<variant>)) AS raw_levels FROM <c>.<s>.orderbook_bronze GROUP BY 1 ORDER BY 1;
SELECT ticker, count_if(gap <> 60) AS gaps FROM (SELECT ticker, unix_timestamp(ts) - lag(unix_timestamp(ts))
  OVER (PARTITION BY ticker ORDER BY ts) AS gap FROM <c>.<s>.orderbook_bronze) WHERE gap IS NOT NULL GROUP BY 1;

-- C. Live latency (seconds from the minute to bronze)
SELECT avg(timestampdiff(MILLISECOND, ts, ingested_at)) / 1000 AS avg_s, max(timestampdiff(MILLISECOND, ts, ingested_at)) / 1000 AS max_s
FROM <c>.<s>.orderbook_bronze WHERE ts >= current_timestamp() - INTERVAL 30 MINUTES;

-- D. Performance over time on real predictions
SELECT to_date(ts) AS d, model_version, count(*) AS n, avg(abs(prediction_bps - actual_bps)) AS mae
FROM <c>.<s>.predictions GROUP BY ALL ORDER BY 1;
```

Expected coefficients (planted by the feed): days 1 and 2 `coef.spread_bps` about +0.40 and `coef.order_imbalance` about 0.80; from day 3, about -0.40 and 0.80.

---

## 8. Changing the model

`train.py` uses every column of `minute_features` except `exchange`, `ticker`, `ts` and the label as a feature. To try other drivers, change the view (and the feed's planted effects, so there is something to find). `train.py`, the deployment job and the scoring job do not change. Keep the rule in `evaluation.py` to one condition unless the user asks for more.

To point at real files instead of the synthetic feed, change the pipeline's `landing_path` configuration to the S3 external location path and leave the feed paused. In production, turn on file events on that external location.

---

## 9. Cleanup (not run in the 2026-11-22 test)

1. `scripts/live.sh stop`.
2. Delete the monitor: `databricks quality-monitors delete <c>.<s>.predictions --profile $PROFILE`.
3. Delete the model: remove the `champion` alias, every version (`databricks model-versions delete`), then `databricks registered-models delete <c>.<s>.next_5min_return_lr`. The model is created by code, not by the bundle, so `bundle destroy` does not know about it.
4. `databricks bundle destroy` removes the jobs, pipeline, volume, experiment and schema it created. Confirm with the user first: it deletes data.
