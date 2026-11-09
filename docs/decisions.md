# Architecture decisions

| Decision | Why | Alternative not taken |
|---|---|---|
| Debezium + Kafka (KRaft, 1 partition per topic) | log-based CDC sees updates and hard deletes; one partition keeps total order | polling `updated_at` (misses deletes) |
| ClickHouse Kafka engine for bronze | ingestion works when dbt is down; bronze is a replayable log | dbt-driven loading |
| Medallion layers with strict rules | isolates PII, keeps logic in one place per layer | single layer of views |
| ReplacingMergeTree + `FINAL` views | cheap "latest version wins" with idempotent appends | UPDATE/DELETE mutations |
| Offset watermark + overlap (20k) + batch cap (1M) | no timestamps, restart-safe, bounded memory | wall-clock windows |
| SCD2 from change events, ASOF join on `created_at` | transaction sees the merchant as it was; no snapshots needed | current attributes (wrong history) |
| Aggregates recomputed per affected day/month | late reversals and merchant corrections stay exact | additive deltas (drift) |
| Python scheduler loop | one process, 30 s cycle, logs to `ops.dbt_runs` | Airflow |
| Metabase on gold only (+ baseline on Postgres) | dashboards cannot touch bronze/silver | admin connection |
| Separate read-only BI users (`bi_ro`) on both databases | dashboards cannot reach bronze, silver or owner columns | admin connection |
| HMAC-SHA256 with a secret for owner identifiers | 10-digit ids and mobile numbers are brute-forceable with a plain hash | plain SHA-256 |

## Where Airflow / Spark would fit
* Airflow: many dependent jobs, backfills with dated parameters, SLAs and alerting, several teams. Here there is one
  loop with one dependency (silver → gold), so Airflow's scheduler, metadata DB and workers (>1 GB RAM) add risk, not value.
* Spark: not justified by 5M rows / ~40 events per second; ClickHouse handles it on one node.

## Assumptions
* Day = Asia/Tehran, month = Gregorian; success rate excludes PENDING; REVERSED is not successful.
* `merchants.updated_at` is the business time of a merchant change; `transactions.created_at` selects the version.
* The first merchant version holds for all earlier times (1970-01-01). MCC and name never change in the simulation.
* Merchant history is synthetic and backdated (commit time is "now", business time is in the past), so lag is
  measured from commit time, never from `valid_from`.