#!/usr/bin/env bash
# Back to the demo's opening state after a rehearsal: @champion = v2, v3 waiting at Approval_Check, and the third day
# scored by v2 in the predictions table. Deletes the UC model (all versions), the experiment's runs and the predictions
# rows, redeploys (approval_mode back to the bundle default), then trains the first three days in order and scores the
# third. Bronze and the landing files are kept. About 15 minutes.
set -euo pipefail
# Locally the CLI uses the profile in PROFILE (default below). In the workspace web terminal it authenticates from the
# environment (DATABRICKS_HOST is set there) and profiles aren't supported, so no profile is set.
[ -n "${DATABRICKS_HOST:-}" ] || export DATABRICKS_CONFIG_PROFILE="${PROFILE:-fevm-francisco-martin-classic}"
cd "$(dirname "$0")/.."
databricks bundle deploy > /dev/null
read -r CATALOG SCHEMA EXPERIMENT <<< "$(databricks bundle summary -o json | python3 -c "
import json, sys; d = json.load(sys.stdin); v = d['variables']; r = d['resources']
print(v['catalog']['value'], v['schema']['value'], r['experiments']['training']['id'])")"
MODEL="$CATALOG.$SCHEMA.next_5min_return_lr"
if databricks registered-models get "$MODEL" > /dev/null 2>&1; then
  # A model can't be deleted while it has versions, so remove the alias and every version first.
  databricks registered-models delete-alias "$MODEL" champion 2>/dev/null || true
  for v in $(databricks model-versions list "$MODEL" -o json | python3 -c "import json, sys; print(' '.join(str(m['version']) for m in json.load(sys.stdin)))"); do
    databricks model-versions delete "$MODEL" "$v"
  done
  databricks registered-models delete "$MODEL" && echo "deleted $MODEL"
fi
databricks experiments delete-runs "$EXPERIMENT" "$(( $(date +%s) * 1000 ))" > /dev/null && echo "deleted the experiment's runs"
databricks experimental aitools tools query "DELETE FROM $CATALOG.$SCHEMA.predictions" > /dev/null 2>&1 \
  && echo "emptied the predictions table" || echo "no predictions table yet"
for day in $(databricks fs ls "dbfs:/Volumes/$CATALOG/$SCHEMA/landing/exchange_1" | sort | head -3); do
  echo "training $day"
  databricks bundle run train_model --params "train_date=$day" > /dev/null
done
for _ in $(seq 40); do  # wait (up to 20 min) for the deployment job to promote v2 before scoring with @champion
  champ=$(databricks api get "/api/2.1/unity-catalog/models/$MODEL?include_aliases=true" 2>/dev/null \
    | python3 -c "import json, sys; print({a['alias_name']: a['version_num'] for a in json.load(sys.stdin).get('aliases', [])}.get('champion', ''))")
  [ "$champ" = "2" ] && break
  sleep 30
done
echo "scoring $day"
databricks bundle run score_predictions --params "score_date=$day" > /dev/null
echo "Done. @champion should be v2, v3 waiting for Approve, and $day scored by v2."
