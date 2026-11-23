# orderbook-ml-starter

**Date:** 2026-11-10, updated 2026-11-22 (20-level book, VARIANT bronze, scoring and drift monitor)

A small Databricks project that shows five components end to end on synthetic data:
**an exchange feed lands one file per minute → Auto Loader in a continuous pipeline appends it to bronze → a daily job trains a
linear regression and logs every coefficient → an MLflow 3 deployment job decides whether the new version becomes `@champion`
→ a daily job scores the day with `@champion` into a predictions table watched by a drift monitor.**

It is a base to read and build on, not a complete or production-grade system. Every piece is kept to its simplest working form.
Deploying it with a coding tool: [`AGENT.md`](AGENT.md). The design notes (`2026-11-10_SPEC.md`) are kept outside the repository.

## What's in it

| # | Piece | What it does |
|---|-------|--------------|
| 0 | `src/feed/exchange_feed.py` · job `exchange_feed` | Pretends to be one exchange: writes one JSON file per minute into the `landing` volume, which stands in for S3. One line per corridor (3 corridors) with the order book, 20 levels a side. A continuous job, paused by default. On the first start it writes the last 3 days at once, then goes live. |
| 1 | `src/orderbook_bronze/transformations/orderbook_bronze.sql` · pipeline `orderbook_bronze` | The whole pipeline: Auto Loader (`read_files`) appends every file to one bronze table, with the top 5 levels a side parsed as arrays (`bids`, `asks`) and the full line kept as `raw` (VARIANT). Continuous, classic compute, single node, Core edition. |
| 1 | `src/training/minute_features.sql` | A view on bronze: two drivers (`spread_bps` from the best bid and ask, `order_imbalance` over the top 5 levels) and the label (`next_5min_return_bps`), computed when it's read. |
| 2 | `src/training/setup.py`, `train.py` · job `train_model` | Setup creates the view and the UC model and links the deployment job. Train fits a `LinearRegression` on one day, logs every coefficient and the holdout error, and registers a version. |
| 3 | `src/deployment/evaluation.py`, `approval_check.py`, `deployment.py` · job `model_deployment` | Runs for every new version: compares it with `@champion`, approves it (automatically or by a person), and moves `@champion`. |
| 4 | `src/scoring/score.py` · job `score_predictions` | Scores one day with `@champion` into the `predictions` table (prediction, actual and model version on every row). The first run creates a data profiling monitor on the table. Daily schedule, paused. |

Configuration lives in `databricks.yml` (variables) and `resources/` (one file per component, plus `schema.yml` for the
schema, the landing volume and the MLflow experiment).

## The model

`next_5min_return_bps = intercept + b1 · spread_bps + b2 · order_imbalance`, trained on the first 18 hours of a day and measured
on the last 6. The feed plants the truth: `b1 = +0.40` and `b2 = 0.80` on days 1 and 2, then from day 3 the spread's effect
**flips to −0.40**. The features are every column of the view except the keys and the label, so to try other variables,
change the view (and the feed); `train.py`, the deployment job and the scoring job don't change.

## Automatic or human approval: one setting

The deployment job has a parameter `approval_mode` (default from the bundle variable of the same name).
Evaluation always applies one rule: **the MAE on the candidate's holdout is at most 5% worse than the champion's, and no
coefficient changed sign.** It writes the result on the version as the `evaluation` tag.

| `approval_mode` | Rule passes | Rule fails |
|---|---|---|
| `auto` | Approved by Evaluation, promoted with no human | Waits at `Approval_Check` until someone clicks **Approve** |
| `human` | Waits for **Approve** | Waits for **Approve** |

Change it in the job's parameters in the Jobs UI, or deploy with `--var approval_mode=human`. **During a live demo use the Jobs
UI:** a deploy also sets the feed job back to paused (see the cost guard).

## Run it

```bash
databricks bundle deploy --profile <profile>   # creates everything; the first deploy starts the pipeline
scripts/live.sh start                          # unpause the feed (and start the pipeline if it isn't running)
# wait until bronze has the backfill, then train one day at a time, oldest first:
databricks bundle run train_model --params train_date=<YYYY-MM-DD> --profile <profile>
# once the deployment job has promoted v2, score day 3 with @champion:
databricks bundle run score_predictions --params score_date=<YYYY-MM-DD> --profile <profile>
scripts/live.sh stop                           # pause the feed and stop the pipeline
scripts/reset_demo.sh                          # after a rehearsal: back to v2 champion, v3 waiting, day 3 scored by v2
```

Train and score only complete days (UTC): training needs more than 100 rows in both the first 18 hours and the last 6.

To use it in another workspace, set the `dev` target's `host`, and the `catalog` and `schema` variable defaults in
`databricks.yml` (not with `--var`: the scripts redeploy without it). `databricks.yml` holds no profile on purpose (see "Working from
the workspace"), so pass `--profile` to every CLI command and export `PROFILE=<profile>` for the scripts. Change
`node_type` if `r5d.xlarge` isn't available. [`AGENT.md`](AGENT.md) has the full checklist, including what to change for a second copy
in the same workspace.

**Cost guard.** The feed and the pipeline run until they are stopped, and the pipeline is classic on-demand compute.
**Creating the pipeline or changing its settings starts it** (a redeploy that only changes SQL doesn't). Run `scripts/live.sh stop`
after every session and check with `scripts/live.sh status`. **Any `bundle deploy` sets the feed job back to `PAUSED`, but a feed run
already in progress keeps writing**, and `live.sh status` then reports `PAUSED`. `live.sh stop` cancels that run too.

## Changing the file format or the bronze columns

The feed carries on from the last file it finds, and it fails on a file in another format. Bronze is a streaming table, so new
columns need one full refresh. The 2026-11-22 upgrade from top-of-book files to the 20-level book took these steps:

```bash
scripts/live.sh stop
databricks fs rm -r dbfs:/Volumes/<catalog>/<schema>/landing/exchange_1 --profile <profile>   # old-format files
databricks bundle deploy --profile <profile>
databricks pipelines start-update <pipeline id> --full-refresh --profile <profile>
scripts/live.sh start          # the feed writes 3 days again from midnight UTC, then goes live
scripts/reset_demo.sh          # once bronze has caught up: new model versions on the new data
```

## Rehearse, then reset

The demo's opening state is `@champion` = v2, v3 waiting at `Approval_Check`, and day 3 scored by v2. Approving v3 uses it up.
`scripts/reset_demo.sh` redeploys (so `approval_mode` is back to the bundle default), deletes the model (all versions), the
experiment's runs and the predictions rows, trains the first three days again and scores the third (about 8 minutes). Bronze and
the landing files are kept. The script stops on the first error, including a network blip while it waits on a run; then wait until
`model_deployment` has no active run and run it again.

## Working from the workspace

The whole project (this README, `AGENT.md`, the spec, `databricks.yml`, `resources/`, `src/` and `scripts/`) is also in a regular
workspace folder, `/Workspace/Users/<you>/orderbook-ml-starter-source`, so every file can be opened and shown in the UI. It is a
copy, separate from the bundle's deployment folder `/Workspace/Users/<you>/orderbook-ml-starter` (`files/`, `state/`).

**Deploy from there with the web terminal** (an admin must enable it; it runs on classic compute with Databricks Runtime 15.0 or
above). In the terminal the CLI authenticates as you from the environment, and profiles aren't supported, which is why
`databricks.yml` has no profile and the scripts only set one when `DATABRICKS_HOST` is unset:

```bash
cd /Workspace/Users/<you>/orderbook-ml-starter-source
databricks bundle deploy
bash scripts/live.sh status        # run the scripts with bash: workspace files don't keep the executable bit
```

A deploy from there uses the same bundle name, target and user as a deploy from a laptop, so it updates the same jobs and
pipeline (one deployment, one state). The **Deploy** button of the workspace bundle editor needs the bundle in a Git folder,
not a regular folder. The deploy from the web terminal has not been tested yet; the 2026-11-22 tests ran from a laptop.

**Keeping the copy current.** The copy is refreshed one way, from the laptop, and a refresh overwrites edits made in the
workspace. After a local change, from this folder:

```bash
databricks sync --include "src/**" --include "resources/**" --include "scripts/**" --include "*.md" --include "databricks.yml" \
  --exclude ".databricks/**" . /Workspace/Users/<you>/orderbook-ml-starter-source --profile <profile>
```

(The `--include` flags are needed because the repository this folder sits in ignores it in `.gitignore`.)

## What got built (fevm-francisco-martin-classic)

| Asset | ID / name |
|-------|-----------|
| Pipeline `[orderbook-ml-starter] orderbook_bronze` | `14430eac-ddc2-4fb6-9e30-00652f477f10` (table `orderbook_bronze`) |
| Job `[orderbook-ml-starter] exchange_feed` | `499236559364000` (continuous trigger, paused) |
| Job `[orderbook-ml-starter] train_model` | `595803664420401` (daily 00:30 UTC, paused) |
| Job `[orderbook-ml-starter] model_deployment` | `1031832955750898`, linked to the model |
| Job `[orderbook-ml-starter] score_predictions` | `573979575598905` (daily 01:00 UTC, paused) |
| Schema / volume | `francisco_martin_catalog.orderbook_ml_starter` / `landing` |
| Tables | `orderbook_bronze`, view `minute_features`, `predictions` (+ monitor tables `predictions_profile_metrics`, `predictions_drift_metrics`) |
| UC model | `next_5min_return_lr` |
| Experiment | `/Users/<you>/orderbook-ml-starter/experiment` (`1592869286900929`) |
| Monitor dashboard | `01f1e6895d671e01af29134c663433da` |
| Source copy (regular folder) | `/Workspace/Users/<you>/orderbook-ml-starter-source` |

### Verification log, 2026-11-22 (current format)
- **Upgrade:** the steps in "Changing the file format" above. Without deleting the old files the feed fails with
  `AttributeError: 'Pandas' object has no attribute 'bids'`.
- **Pipeline:** settings unchanged and as specified. The cluster started in 7 minutes (full refresh) and 8 minutes (cold
  `live.sh start`); the backfill of 15,714 rows reached bronze about a minute after that.
- **Bronze:** 4,320 rows a day, no duplicates, no gaps, 5 parsed levels a side and 20 in `raw`. Live latency 4.3 s on average,
  4.6 s at most, over 27 minutes. After `live.sh stop`, a 4-minute pause and `live.sh start`, the feed filled the gap.
- **Coefficients** (planted +0.40 / 0.80, then −0.40 / 0.80 from 11-21): 11-19 +0.396 / 0.782, 11-20 +0.391 / 0.783,
  11-21 −0.438 / 0.754. Holdout MAE 1.28 to 1.29 bps.
- **Deployment job:** v1 approved (no champion), v2 approved automatically ("MAE 1.293 vs champion v1 1.288"), v3 held
  ("MAE 1.283 vs champion v2 1.425; sign flip: spread_bps +0.39 → -0.44"). With `approval_mode = human`, v4 passed the rule and
  waited; back on `auto`, v5 was promoted with no human.
- **Approve from the CLI:** set the tag and repair the run with its `job_parameters` and, if it was repaired before, the
  `latest_repair_id`. A repair without parameters now fails (`For input string: ""`) instead of promoting a wrong version.
- **Scoring:** 4,320 predictions per day, tagged with the model version; the first run created the monitor, and a second run
  while it was refreshing queued another refresh. v2's MAE was 1.28 on 11-20 and 1.44 on 11-21, after the market change.
- **Reset:** `reset_demo.sh` restored the opening state in about 8 minutes (on the second attempt; the first stopped on a
  network error).
- **Code size:** about 195 lines without markdown, comments or `%pip` (feed 48, scoring 35, evaluation 31, train 29, setup 16,
  view 13, approval check 9, pipeline SQL 7, deployment 7).

### Verification log, 2026-11-10 and 11 (previous format: top of book, flat bronze columns)
- **Pipeline settings (live JSON):** `serverless: false`, `continuous: true`, `edition: CORE`, `photon: false`, no `development`,
  `num_workers: 0`, `r5d.xlarge`, `ON_DEMAND`, `root_path` and the `transformations/**` glob. Created on deploy and started by it;
  the cluster took about 8.5 minutes to start. A redeploy with unchanged pipeline settings left it idle.
- **Feed:** a continuous job on serverless ran one Python loop for 5 hours without a restart. Backfill from 2026-11-07 00:00,
  then live. Bronze: 1,440 minutes a day × 3 corridors, no gaps and no duplicates over 4 full days.
- **Live latency:** over 61 live minutes, files were in bronze 4.3 s after their minute on average, 4.6 s at most. The event log
  shows Databricks' own advisory to use file events instead of directory listing in continuous mode.
- **Coefficients** (planted +0.40 / 0.80, then −0.40 / 0.80 from 11-09): 11-07 +0.42 / 0.86, 11-08 +0.40 / 0.82,
  11-09 −0.36 / 0.80. Holdout MAE 1.31 bps. The metrics are on the run and on the logged model behind each UC version.
- **Deployment job:** v1 approved (no champion), v2 approved automatically ("MAE 1.305 vs champion v1 1.306"), v3 held
  ("MAE 1.315 vs champion v2 1.455; sign flip: spread_bps +0.40 → -0.36: rule failed, waiting for Approve").
  Runs started by a new version receive the job's default `approval_mode` (`auto`).
- **Approve:** setting `Approval_Check = Approved` and repairing the run moved `@champion` to v3. **Repairing from the CLI or
  API needs the run's `job_parameters` passed explicitly**: a bare `repair-run` reran the tasks with the job defaults
  (`model_version` was then `"1"`, and `@champion` moved to v1). The default is now empty, so a repair without parameters
  fails instead. Use the **Approve** button in the UI, and check during the rehearsal that it promotes the right version.
- **The approval switch:** with `approval_mode = human` (deployed with `--var approval_mode=human`), v4 trained on 11-10 passed
  the rule ("MAE 1.163 vs champion v3 1.163: rule passed, waiting for Approve") and its run stopped at `Approval_Check`;
  `@champion` stayed on v3. `scripts/reset_demo.sh` then restored `auto` and the opening state.
- **Quirk:** a task that raises on purpose (`Approval_Check` waiting) ends as `INTERNAL_ERROR` and is retried once. Harmless.
- **Code size** (code lines without markdown or `%pip`): feed 42 (budget 35, mostly widgets and imports), pipeline SQL 5 (8),
  view 11 (12), setup 15 (20), train 28 (40), evaluation 30 (30), approval check 8 (10), deployment 6 (6). Total 145.
- The bundle shows a "Single node cluster is not correctly configured" warning on every command. It is generic cluster
  validation: the pipeline sets the single-node profile itself, as in the 2026-11-06 build.
