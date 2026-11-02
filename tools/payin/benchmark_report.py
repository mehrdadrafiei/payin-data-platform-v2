"""Before/after report from benchmarks/results.csv (written by payin.benchmark).

  python -m payin.benchmark_report [--simulator stopped|running|any]

Uses the LATEST measurement of each label (baseline / optimized). Numbers are Metabase `running_time` per card
execution with the cache bypassed: "first" = run 1, "warm median" = median of runs 2..N. Nothing is estimated.
"""
import argparse
import csv
import os
import statistics

from . import cards as C

OUT_DIR = os.getenv("BENCH_DIR", "/app/benchmarks")


def latest(rows, label, sim):
    sel = [r for r in rows if r["label"] == label and (sim == "any" or r["simulator"] == sim)]
    if not sel:
        raise SystemExit(f"no measurements for label={label!r} simulator={sim!r} in results.csv; "
                         f"run `make benchmark-quiet LABEL={label}` (or `make benchmark LABEL={label}`)")
    stamp = max(r["utc_time"] for r in sel)
    return stamp, [r for r in sel if r["utc_time"] == stamp]


def per_card(rows):
    out = {}
    for c in C.CARDS:
        runs = sorted((r for r in rows if r["card"] == c["key"]), key=lambda r: int(r["run"]))
        t = [float(r["metabase_running_time_ms"]) for r in runs]
        out[c["key"]] = (t[0], statistics.median(t[1:] or t), int(runs[-1]["rows"]), len(t))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--simulator", default="any", choices=["stopped", "running", "any"])
    args = ap.parse_args()
    with open(f"{OUT_DIR}/results.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    b_stamp, b_rows = latest(rows, "baseline", args.simulator)
    o_stamp, o_rows = latest(rows, "optimized", args.simulator)
    b, o = per_card(b_rows), per_card(o_rows)
    b_sim, o_sim = b_rows[0]["simulator"], o_rows[0]["simulator"]

    lines = ["# Benchmark: baseline (PostgreSQL core) vs optimized (ClickHouse gold)", "",
             f"* baseline measured {b_stamp} UTC, simulator: {b_sim}; optimized measured {o_stamp} UTC, "
             f"simulator: {o_sim}; runs per card: {b[C.CARDS[0]['key']][3]} / {o[C.CARDS[0]['key']][3]}.",
             "* Times are Metabase `running_time` in ms, cache bypassed. **First** = run 1; **warm median** = median "
             "of the remaining runs. The OS page cache is not cleared, so `first` is not a true cold start.",
             "* Speedup = baseline warm median / optimized warm median."]
    if b_sim != o_sim:
        lines.append(f"* **WARNING: simulator state differs ({b_sim} vs {o_sim}); the comparison is not like for like.**")
    lines += ["", "| Card | Baseline first | Baseline warm median | Optimized first | Optimized warm median | Speedup | Rows (base / opt) |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for c in C.CARDS:
        k = c["key"]
        lines.append(f"| {c['title']} | {b[k][0]:.0f} | {b[k][1]:.0f} | {o[k][0]:.0f} | {o[k][1]:.0f} | "
                     f"{b[k][1] / max(o[k][1], 1):.1f}x | {b[k][2]} / {o[k][2]} |")
    lines += ["", "## What was optimized", "",
              "* The baseline scans and aggregates millions of rows of `core.transactions` on every dashboard load.",
              "* The optimized cards read small pre-aggregated gold tables (grain: day x channel, month x merchant, "
              "day x MCC x province, month x risk level) that dbt updates incrementally, recomputing only the days "
              "or months that changed. See `docs/data-model.md`.", "",
              "## Intended differences", "",
              "* Card 4 groups by the merchant's province **at transaction time**, card 5 by the merchant's risk level "
              "**at transaction time**; the baseline can only use the current values. `benchmarks/parity.md` proves "
              "the totals still match and quantifies how much moves.",
              "* Row counts of cards 4 and 5 may therefore differ."]
    with open(f"{OUT_DIR}/report.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nwritten: {OUT_DIR}/report.md")


if __name__ == "__main__":
    main()