# Payin data platform (Zibal take-home)

A local analytical platform for a simulated payment company: PostgreSQL → Debezium → Kafka → ClickHouse
(bronze/silver/gold, dbt) → Metabase. All data is synthetic. Everything runs with Docker Compose in 16 GB RAM.

## Run

Prerequisites: Docker Engine with Compose v2, GNU Make, 16 GB RAM, several GB of free disk. The first run pulls/builds
images. All published ports bind to localhost.

    make up                  # builds and starts everything; seeds 2,000 merchants + 5M transactions
    make status              # containers, CDC, bronze counts, Kafka lag, silver/gold lag
    make test                # unit + end-to-end tests (stack must be running)
    make down                # stop, keep data     |  make reset: WARNING, destroys all data volumes
    make bootstrap-metabase  # optional: re-apply the idempotent Metabase setup (normally runs by itself)

The first start takes several minutes (seed, Debezium snapshot, first dbt runs). Wait until `make status` shows Kafka
lag 0 and silver/gold offsets close to bronze before checking dashboards. The E2E suite also waits for the snapshot
and the pipeline itself.

Metabase: http://localhost:3000, `admin@payin.local` / `Payin-demo-2026!` (demo credential). Collection
"Payin Data Platform": **baseline (core)** queries PostgreSQL directly, **optimized (gold)** queries ClickHouse.
Docker Hub blocked? `HUB_REGISTRY=<mirror> make up`.

## Architecture

    Postgres core.* --Debezium--> Kafka --ClickHouse Kafka engine--> bronze --dbt--> silver --dbt--> gold --> Metabase
    (ingestion plane: no dependency on dbt)                         (transformation plane: scheduler.py loop, 30 s)

* **Bronze**: append-only log of every change event (offset, op, LSN, full JSON). Only layer with clear-text PII.
* **Silver**: typed, de-duplicated events and latest state per key; owner identity as HMAC tokens.
* **Gold**: SCD2 `dim_merchant`, `fct_transactions`, four incremental aggregates; merchant attributes at transaction time.
* Updates and deletes: latest `(LSN, offset)` wins; deletes arrive as events and hide the key from current views.
  Duplicates are harmless (event identity `(key, LSN, op)`, ReplacingMergeTree, idempotent models). The new column
  `installments` flows through without a restart.

Details: `docs/data-model.md` (layers, grains, lineage), `docs/cdc-strategy.md`, `docs/decisions.md` (why no Airflow/Spark).

## Data and metrics

Seed: 2,000 merchants, 5M transactions over 180 days, about 3,000 backdated **synthetic** merchant changes; the simulator
adds ~20 transactions/s, reversals, hard-deleted test rows and a merchant change per minute (`docs/synthetic-data.md`).
Day = Asia/Tehran, month = Gregorian. Success rate = SUCCESS / (SUCCESS + FAILED + REVERSED). Cards 4 and 5 use the
merchant's province / risk level **at transaction time** (SCD2 + ASOF join on `created_at`); the baseline can only use
current values (`docs/metrics.md`).

## Verify and reproduce

    make reset && make up        # only if you accept deleting all data
    make status                  # wait for catch-up
    make test                    # unit + E2E: CDC, schema change, silver/gold, PII, access
    make dbt-test                # dbt key / SCD2 / aggregate tests
    make parity-quiet            # source vs silver vs gold, baseline vs optimized -> benchmarks/parity.md
    make benchmark-quiet LABEL=baseline
    make benchmark-quiet LABEL=optimized
    make benchmark-report        # -> benchmarks/report.md

Last run (author's machine): dbt 37/37, unit 6, e2e 25, parity all PASS.

Benchmark: measured on the author's machine, simulator stopped, 6 runs per card, cache bypassed, warm median Metabase
time. Rerun the commands above for your own numbers.

| Card | Core → gold (ms) | Speedup | Optimization |
|---|---:|---:|---|
| 1. Daily successful amount/count | 2247 → 72 | 31.2× | Day × channel aggregate |
| 2. Daily success rate by channel | 1004 → 62 | 16.2× | Per-status counts from the same aggregate |
| 3. Top 10 merchants this month | 87 → 66 | 1.3× | Month × merchant aggregate |
| 4. Fees by MCC/province | 1550 → 62 | 25.0× | Day × MCC × transaction-time province |
| 5. Monthly volume by risk | 5464 → 53 | 103.1× | Month × transaction-time risk level |

Card 3 gains little because the baseline is already fast: the `created_at` index and a small current-month window.

## Resources

Compose limits: Postgres 1.5 GB, Kafka 1 GB, Connect 1.25 GB, ClickHouse 4 GB (server cap 3 GB), Metabase 1.5 GB,
transformer 0.75 GB, small jobs ≤ 0.25 GB each (about 11 GB in total). `make mem` shows actual use.

## Governance (`docs/governance.md`)

Owner national id and mobile exist in clear text only in the source and bronze. Silver holds HMAC-SHA256 tokens
(secret in a Docker volume, not in Git) and a masked mobile; `gold.dim_merchant` keeps one linkable pseudonymous token
that no card uses and that is still treated as sensitive. Dashboard users are read-only: ClickHouse `bi_ro` sees only
`gold`; PostgreSQL `bi_ro` cannot read owner columns.

## Assumptions and limitations

Day is Tehran-local, month Gregorian; SUCCESS excludes PENDING and REVERSED; merchant `updated_at` is business time; the
first known merchant state is valid from 1970. Single-node demo: no secret rotation, retention/deletion controls,
`TRUNCATE` capture or merchant-delete history. Kafka keeps 3 days; a longer outage can lose events not yet in bronze.
. Demo credentials are static on purpose.
More: `docs/decisions.md`, `docs/data-model.md`, `docs/cdc-strategy.md`.