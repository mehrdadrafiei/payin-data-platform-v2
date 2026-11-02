# Benchmark: baseline (PostgreSQL core) vs optimized (ClickHouse gold)

* baseline measured 2026-11-02T19:28:58+00:00 UTC, simulator: stopped; optimized measured 2026-11-02T19:30:26+00:00 UTC, simulator: stopped; runs per card: 6 / 6.
* Times are Metabase `running_time` in ms, cache bypassed. **First** = run 1; **warm median** = median of the remaining runs. The OS page cache is not cleared, so `first` is not a true cold start.
* Speedup = baseline warm median / optimized warm median.

| Card | Baseline first | Baseline warm median | Optimized first | Optimized warm median | Speedup | Rows (base / opt) |
|---|---:|---:|---:|---:|---:|---:|
| 1. Daily successful amount and count (last 90 days) | 2825 | 2247 | 87 | 72 | 31.2x | 90 / 90 |
| 2. Daily success rate by channel (last 30 days) | 1043 | 1004 | 67 | 62 | 16.2x | 90 / 90 |
| 3. Top 10 merchants this month by successful amount | 104 | 87 | 64 | 66 | 1.3x | 10 / 10 |
| 4. Fees by merchant category (MCC) and province (last 90 days) | 1546 | 1550 | 62 | 62 | 25.0x | 180 / 180 |
| 5. Monthly successful volume by merchant risk level | 5872 | 5464 | 58 | 53 | 103.1x | 21 / 21 |

## What was optimized

* The baseline scans and aggregates millions of rows of `core.transactions` on every dashboard load.
* The optimized cards read small pre-aggregated gold tables (grain: day x channel, month x merchant, day x MCC x province, month x risk level) that dbt updates incrementally, recomputing only the days or months that changed. See `docs/data-model.md`.

## Intended differences

* Card 4 groups by the merchant's province **at transaction time**, card 5 by the merchant's risk level **at transaction time**; the baseline can only use the current values. `benchmarks/parity.md` proves the totals still match and quantifies how much moves.
* Row counts of cards 4 and 5 may therefore differ.
