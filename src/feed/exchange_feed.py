# Databricks notebook source
# MAGIC %md
# MAGIC # 0 · Exchange feed
# MAGIC Pretends to be one exchange. Every minute it writes one JSON file with the order book (20 levels a side) for three
# MAGIC corridors into the `landing` volume, which stands in for the S3 bucket the Lambdas write to:
# MAGIC `landing/<exchange>/<YYYY-MM-DD>/<HHMM>.json`, one line per corridor:
# MAGIC `{"exchange": ..., "ticker": ..., "ts": ..., "bids": [[price, size], ...], "asks": [[price, size], ...]}`
# MAGIC
# MAGIC - **First start:** the folder is empty, so it writes the last 3 days at once (from midnight UTC), then goes live.
# MAGIC - **Every start after that:** it carries on from the last file, so a pause leaves no gap.
# MAGIC - **The planted truth:** each minute's `spread_bps` (best ask vs. best bid) and `order_imbalance` (over the top 5
# MAGIC   levels) move the price over the next minutes. From day 3 the market changes and the spread's effect flips sign.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("landing_volume", "landing")
dbutils.widgets.text("exchange", "exchange_1")
dbutils.widgets.text("backfill_days", "3")
catalog, schema, volume, exchange = (dbutils.widgets.get(k) for k in ["catalog", "schema", "landing_volume", "exchange"])
FOLDER = f"/Volumes/{catalog}/{schema}/{volume}/{exchange}"

# COMMAND ----------

import os
import time

import numpy as np
import pandas as pd

TICKERS = {"USD-MXN": 18.0, "USD-BRL": 5.5, "USD-PHP": 58.0}  # starting mid prices
LEVELS = 20                                                     # levels a side in the raw book; the model uses the top 5
REGIME_A = {"spread_bps": 0.40, "order_imbalance": 0.80}       # days 1 and 2
REGIME_B = {"spread_bps": -0.40, "order_imbalance": 0.80}      # day 3 onward, including live
rng = np.random.default_rng()

os.makedirs(FOLDER, exist_ok=True)
days = sorted(os.listdir(FOLDER))
if days:  # carry on from the last file, at its prices
    last = sorted(os.listdir(f"{FOLDER}/{days[-1]}"))[-1]
    t = pd.Timestamp(f"{days[-1]} {last[:2]}:{last[2:4]}", tz="UTC") + pd.Timedelta(minutes=1)
    prev = pd.read_json(f"{FOLDER}/{days[-1]}/{last}", lines=True)
    mid = {r.ticker: (r.bids[0][0] + r.asks[0][0]) / 2 for r in prev.itertuples()}
else:     # first start: begin at midnight, backfill_days ago
    t = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=int(dbutils.widgets.get("backfill_days")))
    days, mid = [f"{t:%Y-%m-%d}"], dict(TICKERS)
shift = pd.Timestamp(days[0], tz="UTC") + pd.Timedelta(days=2)  # the market changes on day 3
push = dict.fromkeys(TICKERS, 0.0)                              # last minute's drivers, applied to this minute
print(f"Writing from {t} to {FOLDER}")

# COMMAND ----------

def book(m, half, sizes, sign):
    """One side of the book: [price, size] pairs, 0.5 bps apart, starting half a spread from the mid."""
    return [[round(m + sign * (half + k * m * 0.5e-4), 6), round(float(s))] for k, s in enumerate(sizes)]


while True:
    time.sleep(max(0.0, (t - pd.Timestamp.now(tz="UTC")).total_seconds()))  # live: wait for the minute
    beta, rows = (REGIME_B if t >= shift else REGIME_A), []
    for ticker in TICKERS:
        mid[ticker] *= np.exp((push[ticker] + rng.normal(0, 0.5)) / 1e4)   # price move this minute, in bps
        spread, imbalance = rng.uniform(0.5, 3.5), rng.uniform(-1, 1)      # this minute's drivers
        push[ticker] = beta["spread_bps"] * (spread - 2.0) + beta["order_imbalance"] * imbalance
        top5 = rng.dirichlet(np.ones(5)) * rng.lognormal(0, 0.3) * 1e6      # top-5 depth, split so its imbalance is planted
        deep = rng.lognormal(12, 0.5, (2, LEVELS - 5))                      # levels 6 to 20: kept in raw, unused by the model
        half = mid[ticker] * spread / 2e4
        rows.append({"exchange": exchange, "ticker": ticker, "ts": f"{t:%Y-%m-%dT%H:%M:%SZ}",
                     "bids": book(mid[ticker], half, np.r_[top5 * (1 + imbalance), deep[0]], -1),
                     "asks": book(mid[ticker], half, np.r_[top5 * (1 - imbalance), deep[1]], +1)})
    os.makedirs(f"{FOLDER}/{t:%Y-%m-%d}", exist_ok=True)
    pd.DataFrame(rows).to_json(f"{FOLDER}/{t:%Y-%m-%d}/{t:%H%M}.json", orient="records", lines=True)
    t += pd.Timedelta(minutes=1)
