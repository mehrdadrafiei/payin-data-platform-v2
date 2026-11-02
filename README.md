# Payin data platform (Zibal take-home)

A local analytical platform for a simulated payment company: PostgreSQL → Debezium → Kafka → ClickHouse
(bronze/silver/gold, dbt) → Metabase. All data is synthetic. Everything runs with Docker Compose in 16 GB RAM.

## Run

    make up          # one command: builds, seeds 2,000 merchants + 5M transactions, starts CDC, simulator, dbt, Metabase
    make status      # containers, connector, bronze counts, Kafka lag, silver/gold lag
    make down        # stop (make reset destroys all data)

First start takes several minutes (seed, Debezium snapshot, first dbt runs; `make status` shows when lag is 0).
Metabase: http://localhost:3000, `admin@payin.local` / `Payin-demo-2026!` (demo credential). Two dashboards in the
collection "Payin Data Platform": **baseline (core)** queries PostgreSQL directly, **optimized (gold)** queries ClickHouse.
Docker Hub blocked? `HUB_REGISTRY=<mirror> make up`.

## Architecture

    Postgres core.* --Debezium--> Kafka --ClickHouse Kafka engine--> bronze --dbt--> silver --dbt--> gold --> Metabase
    (ingestion plane: no dependency on dbt)                         (transformation plane: scheduler.py loop, 30 s)

| Layer | Content | Rule |
|---|---|---|
| bronze | immutable log of every change event (offset, op, LSN, full JSON) | only place with raw PII; no logic |
| silver | typed, de-duplicated events; current state; PII as HMAC tokens | no joins, no business rules |
| gold | SCD2 `dim_merchant`, `fct_transactions`, 4 incremental aggregates | point-in-time attributes, no PII |

Updates and deletes: latest `(LSN, offset)` wins; deletes arrive as events and hide the key from current views.
Duplicates are harmless (event identity `(key, LSN, op)`, ReplacingMergeTree, idempotent models). The new column
`installments` flows through without a restart. Details: `docs/cdc-strategy.md`, `docs/data-model.md`.

## Metrics (`docs/metrics.md`)

Day = Asia/Tehran, month = Gregorian. Success rate = SUCCESS / (SUCCESS + FAILED + REVERSED). Cards 4 and 5 use the
merchant's province / risk level **at transaction time** (SCD2 + ASOF join on `created_at`); the baseline can only use
current values. The merchant history is **synthetic** (`docs/synthetic-data.md`).

## Verify

| Command | What it proves |
|---|---|
| `make test-unit` | simulator / history logic |
| `make test-e2e` | CDC, silver, gold, PII tokens, SCD2, aggregates, dashboard-user access (needs the stack, ~3 min) |
| `make dbt-test` | 37 dbt data tests (keys, SCD2 gaps, aggregate == fact) |
| `make parity-quiet` | baseline vs optimized cards, source vs silver vs gold reconciliation → `benchmarks/parity.md` |
| `make benchmark-quiet LABEL=baseline` then `LABEL=optimized`, `make benchmark-report` | timings → `benchmarks/report.md` |

Verification status (fill in after the final clean-room run): dbt __/__, unit __, e2e __, parity __, benchmark: see report.

## Resources

Compose memory limits: Postgres 1.5 GB, Kafka 1 GB, Connect 1.25 GB, ClickHouse 4 GB (server cap 3 GB), Metabase 1.5 GB,
transformer 0.75 GB, small jobs ≤ 0.25 GB each (about 11 GB of limits in total). `make mem` shows actual use.

## Governance (`docs/governance.md`)

Owner national id and mobile exist in clear text only in the source and bronze. Silver/gold hold HMAC-SHA256 tokens
(secret in a Docker volume, not in Git) and a masked mobile. Dashboard users are read-only: ClickHouse `bi_ro` sees
only `gold`; PostgreSQL `bi_ro` cannot read owner columns.

## Decisions and limitations

`docs/decisions.md` (why no Airflow / Spark, assumptions) and the limitation lists in `docs/data-model.md` and
`docs/cdc-strategy.md`. Main ones: a very long outage beyond Kafka's 3-day retention loses events not yet in bronze;
a merchant version arriving out of order does not re-assign existing facts (the overlap re-read makes it unlikely);
demo credentials are in the repository on purpose.