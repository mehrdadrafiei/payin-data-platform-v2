#!/usr/bin/env bash
# One-screen overview: jobs, connector, source counts, bronze counts, Kafka lag.
set -uo pipefail
CH="docker compose exec -T clickhouse clickhouse-client --user admin --password ch_pw --format PrettyCompact"

echo "== containers (one-shot jobs end with 'Exited (0)') =="
docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'

echo; echo "== Debezium connector =="
curl -s http://localhost:8083/connectors/payin-cdc/status 2>/dev/null | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("connector:", d["connector"]["state"], "| tasks:", [t["state"] for t in d["tasks"]])' 2>/dev/null || echo "not registered yet"

echo; echo "== Postgres (source of truth right now) =="
docker compose exec -T postgres psql -U payin -d payin -tA -F ' = ' -c \
  "select 'merchants', count(*) from core.merchants union all select 'transactions', count(*) from core.transactions"

echo; echo "== ClickHouse bronze (change events received, by type) =="
$CH -q "SELECT * FROM (
          SELECT 'merchants' AS tbl, op, count() AS events FROM bronze.merchants_cdc GROUP BY op
          UNION ALL
          SELECT 'transactions', op, count() FROM bronze.transactions_cdc GROUP BY op
        ) ORDER BY tbl, op"
$CH -q "SELECT count() AS broken_messages FROM bronze.cdc_errors"

echo; echo "== Kafka consumer lag (ClickHouse is the consumer; LAG 0 = caught up) =="
for g in ch_bronze_merchants ch_bronze_transactions; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group "$g" 2>/dev/null | grep -E "GROUP|payin" || echo "$g: no offsets yet"
done

echo; echo "== transformer: how far silver and gold have caught up (offsets; equal = caught up) =="
$CH -q "SELECT
          (SELECT max(_offset) FROM bronze.transactions_cdc) AS bronze,
          (SELECT max(kafka_offset) FROM silver.stg_transactions_events) AS silver_events,
          (SELECT max(kafka_offset) FROM silver.transactions_state) AS silver_state,
          (SELECT max(src_kafka_offset) FROM gold.fct_transactions_state) AS gold_fact" 2>&1 | head -8 || true
echo "-- merchants:"
$CH -q "SELECT
          (SELECT max(_offset) FROM bronze.merchants_cdc) AS bronze,
          (SELECT max(kafka_offset) FROM silver.stg_merchants_events) AS silver_events,
          (SELECT max(kafka_offset) FROM silver.merchants_state) AS silver_state,
          (SELECT max(first_kafka_offset) FROM gold.dim_merchant) AS gold_dim_last_version" 2>&1 | head -8 || true
echo "-- physical rows (approximate: old versions are removed by background merges):"
$CH -q "SELECT database, table, sum(rows) AS rows FROM system.parts WHERE active AND database IN ('silver','gold') GROUP BY database, table ORDER BY database, table" 2>&1 | head -30 || true
echo "-- last transformer runs:"
$CH -q "SELECT started_at, kind, duration_s, ok, summary FROM ops.dbt_runs ORDER BY started_at DESC LIMIT 6" 2>&1 | head -12 || true
