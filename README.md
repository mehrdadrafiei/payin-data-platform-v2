# Payin data platform (Zibal take-home)

A local analytical platform for a simulated payment company: PostgreSQL → Debezium → Kafka → ClickHouse
(bronze/silver/gold, dbt) → Metabase. All data is synthetic. Everything runs with Docker Compose in 16 GB RAM.

## Run

Prerequisites: Docker Engine with the Compose v2 plugin, GNU Make, 16 GB RAM, and several GB of free disk space.
The first run also needs access to pull/build the container images. All published service ports bind to localhost.

    make up          # builds and starts the full stack; seeds 2,000 merchants + 5M transactions
    make status      # check containers, CDC, bronze counts, Kafka lag, and silver/gold lag
    make bootstrap-metabase  # optional: rerun the idempotent Metabase setup
    make test        # unit + end-to-end tests (stack must be running)
    make down        # stop the stack, preserving data volumes
    make reset       # WARNING: stop and destroy all data volumes

The first seed, Debezium snapshot, and initial transformations take several minutes. `make up` starts the
Metabase bootstrap automatically; wait for the full initial snapshot and silver/gold catch-up in `make status`
before running tests or checking the dashboards. Healthy containers and a RUNNING connector do not mean the
initial snapshot is complete. In `make status`, wait until the transaction Kafka consumer lag is 0 and the
bronze transaction snapshot (`op = r`) is about 5,000,000 rows; then confirm silver/gold offsets are caught up
or close to bronze (the simulator continues producing changes). Starting E2E tests earlier can cause transient
failures: the snapshot-completeness test compares against all seeded rows, and the card-3 access test needs
current-month rows to have reached gold. A rerun may pass simply because ingestion caught up in the meantime.
The bootstrap job should finish successfully (visible in `make status` or
`docker compose logs metabase-bootstrap`). You do **not** need `make bootstrap-metabase` for a normal fresh start;
run it to reapply the idempotent Metabase setup if cards or dashboards need to be restored.
Metabase: http://localhost:3000, `admin@payin.local` / `Payin-demo-2026!` (demo credential). Two dashboards in the
collection "Payin Data Platform": **baseline (core)** queries PostgreSQL directly, **optimized (gold)** queries ClickHouse.
Docker Hub blocked? `HUB_REGISTRY=<mirror> make up`.

## Architecture

    Postgres core.* --Debezium--> Kafka --ClickHouse Kafka engine--> bronze --dbt--> silver --dbt--> gold --> Metabase
    (ingestion plane: no dependency on dbt)                         (transformation plane: scheduler.py loop, 30 s)

| Choice | Why | Considered but not used |
|---|---|---|
| PostgreSQL + Debezium + Kafka | Source OLTP plus ordered log-based inserts, updates, and deletes | Polling `updated_at` misses hard deletes |
| ClickHouse Kafka engine + bronze | Ingestion is independent of transformations; bronze is replayable | dbt-driven ingestion |
| ClickHouse + dbt medallion models | Incremental analytical state and serving aggregates fit the data size | Spark is unnecessary at 5M rows / ~40 events/s |
| Python 30-second scheduler | One small transformation loop; run history is logged | Airflow is unnecessary for this dependency graph |
| Metabase on gold, plus a core baseline | Comparable before/after cards with restricted BI users | BI access using admin credentials |

| Layer | Holds / allowed | Not allowed |
|---|---|---|
| Bronze | Append-only CDC envelopes with offsets, LSNs, and full JSON; ingestion writes only | Parsing, mutation, TTL, dashboard access |
| Silver | Typed/de-duplicated event history and latest-key state; HMAC/masking of owner identifiers | Clear-text owner identifiers, cross-table joins, business metrics |
| Gold | SCD2 dimension, transaction fact, and incremental serving aggregates; point-in-time joins | Raw events or direct owner identifiers; the dimension's HMAC token remains sensitive |

Updates and deletes: latest `(LSN, offset)` wins; deletes arrive as events and hide the key from current views.
Duplicates are harmless (event identity `(key, LSN, op)`, ReplacingMergeTree, idempotent models). The new column
`installments` flows through without a restart. Details: `docs/cdc-strategy.md`, `docs/data-model.md`.

## Data and analytical model

The seed creates 2,000 merchants and 5,000,000 transactions over 180 days (80% SUCCESS, 10% FAILED, 10% REVERSED).
It also creates about 3,000 backdated synthetic merchant changes for province/risk history. The live simulator inserts
about 20 PENDING transactions/s, resolves them after 2–10s, reverses some, hard-deletes short-lived test rows, and
changes one merchant's province/risk each minute. `docs/synthetic-data.md` documents the generator and restart behavior.

| Model | Grain |
|---|---|
| `bronze.*_cdc` | One source change event (including deletes) |
| `silver.stg_*_events` / `*_events` | One typed source event per key, LSN, and operation |
| `silver.*_state` / `*_current` | Latest state per merchant or transaction key; current views hide deletes |
| `gold.dim_merchant` | One merchant per SCD2 version |
| `gold.fct_transactions` | One current, non-deleted transaction with merchant attributes at transaction time |
| `gold.agg_daily_channel` | Tehran day × channel (cards 1–2) |
| `gold.agg_merchant_month` | Tehran month × merchant (card 3) |
| `gold.agg_fee_day_mcc_province` | Tehran day × MCC × transaction-time province (card 4) |
| `gold.agg_month_risk` | Tehran month × transaction-time risk level (card 5) |

## Metrics (`docs/metrics.md`)

Day = Asia/Tehran, month = Gregorian. Success rate = SUCCESS / (SUCCESS + FAILED + REVERSED). Cards 4 and 5 use the
merchant's province / risk level **at transaction time** (SCD2 + ASOF join on `created_at`); the baseline can only use
current values. The merchant history is **synthetic** (`docs/synthetic-data.md`).

## Verify and reproduce

For a clean local run, use `make reset` **only if you are okay deleting all project data**, then:

    make up
    make status                 # wait for the snapshot + transformations to catch up (see readiness note above)
    make test                   # unit + stack E2E: CDC, schema change, silver/gold, privacy, access
    make dbt-test                # dbt key/value/SCD2/aggregate tests
    make parity-quiet            # exact source/card reconciliation; pauses simulator temporarily
    make benchmark-quiet LABEL=baseline
    make benchmark-quiet LABEL=optimized
    make benchmark-report
    make down                    # stop, keep volumes

The E2E schema test adds `installments` if absent, checks PostgreSQL and Debezium remain usable, and waits for its
value to reach bronze, silver, and gold. `make add-installments` is an optional manual demo; use
`make add-installments-default` instead only to demonstrate the documented old-row default/backfill caveat.
These are alternatives, not steps required for normal startup. `make up` runs Metabase bootstrap automatically;
`make bootstrap-metabase` is only for reapplying that setup.

The checked-in benchmark is a six-run, simulator-stopped comparison; cache bypassed, warm median Metabase execution
times. Actions are the gold grains queried by each card; parity separately checks intended point-in-time differences.

| Card | Core → gold warm median | Speedup | Optimization |
|---|---:|---:|---|
| 1. Daily successful amount/count | 2247 → 72 ms | 31.2× | Aggregate by day/channel, roll up by day |
| 2. Daily success rate by channel | 1004 → 62 ms | 16.2× | Read per-status counts from day/channel aggregate |
| 3. Top 10 merchants this month | 87 → 66 ms | 1.3× | Monthly merchant aggregate + current name |
| 4. Fees by MCC/province | 1550 → 62 ms | 25.0× | Day/MCC/transaction-time-province aggregate |
| 5. Monthly volume by risk | 5464 → 53 ms | 103.1× | Month/transaction-time-risk aggregate |

Numbers are recorded example runs, not a result from your machine; see `benchmarks/report.md` and `benchmarks/parity.md`.

## Resources

Compose memory limits: Postgres 1.5 GB, Kafka 1 GB, Connect 1.25 GB, ClickHouse 4 GB (server cap 3 GB), Metabase 1.5 GB,
transformer 0.75 GB, small jobs ≤ 0.25 GB each (about 11 GB of limits in total). `make mem` shows actual use.

## Governance (`docs/governance.md`)

Owner national id and mobile exist in clear text only in the source and bronze. Silver/gold hold HMAC-SHA256 tokens
(secret in a Docker volume, not in Git) and a masked mobile. Dashboard users are read-only: ClickHouse `bi_ro` sees
only `gold`; PostgreSQL `bi_ro` cannot read owner columns.

## Decisions and limitations

Assumptions: day is Tehran-local, month is Gregorian; SUCCESS excludes PENDING and REVERSED; merchant `updated_at`
is business-effective time; first known merchant state is assumed valid from 1970. Merchant history is synthetic and
backdated. This single-node demo does not include production secret rotation, formal retention/deletion controls,
`TRUNCATE` capture, or merchant-delete history. Kafka retention is three days; an outage longer than retention can
lose events not yet in bronze. Demo credentials are intentionally static. Further rationale and limitations:
`docs/decisions.md`, `docs/data-model.md`, `docs/cdc-strategy.md`, and `docs/governance.md`.