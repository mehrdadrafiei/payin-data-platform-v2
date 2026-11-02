#!/usr/bin/env bash
# Waits until the long-running services report "healthy".
set -euo pipefail
timeout="${1:-300}"
start=$(date +%s)
for s in postgres kafka connect clickhouse metabase; do
  printf "waiting for %-10s" "$s"
  until cid=$(docker compose ps -q "$s") && [ -n "$cid" ] && \
        [ "$(docker inspect -f '{{.State.Health.Status}}' "$cid" 2>/dev/null)" = "healthy" ]; do
    if [ $(( $(date +%s) - start )) -gt "$timeout" ]; then
      echo " TIMEOUT"; docker compose logs --tail=40 "$s"; exit 1
    fi
    printf "."; sleep 3
  done
  echo " healthy"
done
