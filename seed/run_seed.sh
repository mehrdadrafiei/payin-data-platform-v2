#!/usr/bin/env bash
# Idempotent base seed. Runs once; later starts see the marker and exit immediately.
set -euo pipefail
N_TXN="${SEED_N_TXN:-5000000}"
N_MERCHANTS="${SEED_N_MERCHANTS:-2000}"

psql -v ON_ERROR_STOP=1 -q -c "CREATE TABLE IF NOT EXISTS public._seed_state (
    key text PRIMARY KEY, value text NOT NULL, at timestamptz NOT NULL DEFAULT now())"

if [ "$(psql -tA -c "SELECT count(*) FROM public._seed_state WHERE key = 'base_done'")" = "1" ]; then
  echo "[seed] base data already present, skipping"
  exit 0
fi

echo "[seed] generating ${N_MERCHANTS} merchants and ${N_TXN} transactions (a few minutes)..."
start=$(date +%s)
psql -v ON_ERROR_STOP=1 -v n_txn="${N_TXN}" -v n_merchants="${N_MERCHANTS}" -f /seed/seed_base.sql
echo "[seed] finished in $(( $(date +%s) - start )) s"
