#!/usr/bin/env bash
# Start, stop or check the two always-on pieces: the exchange_feed job and the orderbook_bronze pipeline.
# Usage: scripts/live.sh start|stop|status   (locally, override the profile with PROFILE=...)
# Both run until stopped, so stop them after every setup, rehearsal and demo.
set -euo pipefail
# Locally the CLI uses the profile in PROFILE (default below). In the workspace web terminal it authenticates from the
# environment (DATABRICKS_HOST is set there) and profiles aren't supported, so no profile is set.
[ -n "${DATABRICKS_HOST:-}" ] || export DATABRICKS_CONFIG_PROFILE="${PROFILE:-fevm-francisco-martin-classic}"
cd "$(dirname "$0")/.."
read -r PIPELINE JOB <<< "$(databricks bundle summary -o json | python3 -c "
import json, sys; r = json.load(sys.stdin)['resources']
print(r['pipelines']['orderbook_bronze']['id'], r['jobs']['exchange_feed']['id'])")"
feed() { databricks jobs update --json "{\"job_id\": $JOB, \"new_settings\": {\"continuous\": {\"pause_status\": \"$1\"}}}"; }
case "${1:-status}" in
  start) databricks pipelines start-update "$PIPELINE" > /dev/null 2>&1 || true  # already running is fine
         feed UNPAUSED ;;
  stop)  feed PAUSED
         databricks jobs cancel-all-runs --job-id "$JOB" > /dev/null
         databricks pipelines stop "$PIPELINE" > /dev/null ;;
esac
databricks pipelines get "$PIPELINE" -o json | python3 -c "
import json, sys; d = json.load(sys.stdin); u = (d.get('latest_updates') or [{}])[0]
print('orderbook_bronze:', d.get('state'), '| latest update:', u.get('state'))"
databricks jobs get "$JOB" -o json | python3 -c "
import json, sys; print('exchange_feed:', json.load(sys.stdin)['settings']['continuous']['pause_status'])"
