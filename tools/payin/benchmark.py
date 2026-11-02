"""Times every dashboard card through the Metabase API (what a dashboard user actually waits for).

  python -m payin.benchmark --label baseline            # cards named "[core] ..."
  python -m payin.benchmark --label baseline --explain  # also writes EXPLAIN (ANALYZE, BUFFERS) of each baseline query

Each card runs `--runs` times with the cache bypassed. Run 1 is reported separately ("first"); the rest are "warm".
The operating system's page cache is NOT cleared, so "first" is not a true cold start; the README says so.
Results are appended to benchmarks/results.csv and summarized in benchmarks/<label>.md.
"""
import argparse
import csv
import datetime as dt
import os
import statistics
import time

import psycopg

from . import cards as C
from .metabase_api import Metabase

OUT_DIR = os.getenv("BENCH_DIR", "/app/benchmarks")
PREFIXES = {"baseline": C.BASELINE_PREFIX, "optimized": "[gold] "}


def run_card(mb, card_id):
    t0 = time.perf_counter()
    res = mb.call("POST", f"/api/card/{card_id}/query", {"ignore_cache": True})
    wall_ms = (time.perf_counter() - t0) * 1000
    if res.get("status") != "completed" or res.get("error"):
        raise RuntimeError(f"card {card_id} failed: {res.get('error')}")
    return res.get("running_time", 0), wall_ms, res.get("row_count", len(res["data"]["rows"]))


def explain_all(path):
    with psycopg.connect(autocommit=True) as conn, open(path, "w") as out:
        for c in C.CARDS:
            out.write(f"===== {c['title']} =====\n{c['core_sql']}\n\n")
            for (line,) in conn.execute("EXPLAIN (ANALYZE, BUFFERS) " + c["core_sql"]):
                out.write(line + "\n")
            out.write("\n")
    print(f"EXPLAIN ANALYZE plans written to {path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", choices=sorted(PREFIXES), required=True)
    ap.add_argument("--runs", type=int, default=6)
    ap.add_argument("--explain", action="store_true")
    args = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)

    mb = Metabase()
    mb.wait_until_ready(60)
    mb.login()
    by_name = {c["name"]: c["id"] for c in mb.cards()}
    sim_state = os.getenv("SIM_STATE", "unknown")
    stamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    rows, csv_rows = [], []
    for c in C.CARDS:
        name = PREFIXES[args.label] + c["title"]
        if name not in by_name:
            raise SystemExit(f"card not found in Metabase: {name!r} (run the bootstrap first)")
        runs = [run_card(mb, by_name[name]) for _ in range(args.runs)]
        server = [r[0] for r in runs]
        first, warm = server[0], server[1:] or server
        rows.append((c["title"], first, statistics.median(warm), min(warm), max(warm), runs[-1][2]))
        for i, (srv, wall, n) in enumerate(runs, 1):
            csv_rows.append([stamp, args.label, sim_state, c["key"], i, round(srv), round(wall), n])
        print(f"{c['title'][:58]:58} first {first:>7.0f} ms | warm median {statistics.median(warm):>7.0f} ms")

    new = not os.path.exists(f"{OUT_DIR}/results.csv")
    with open(f"{OUT_DIR}/results.csv", "a", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        if new:
            w.writerow(["utc_time", "label", "simulator", "card", "run", "metabase_running_time_ms", "wall_ms", "rows"])
        w.writerows(csv_rows)

    with open(f"{OUT_DIR}/{args.label}.md", "w") as f:
        f.write(f"Benchmark `{args.label}` at {stamp} UTC, simulator: {sim_state}, {args.runs} runs per card "
                f"(cache bypassed; times are Metabase `running_time` in ms)\n\n")
        f.write("| Card | First run | Warm median | Warm min | Warm max | Rows |\n|---|---:|---:|---:|---:|---:|\n")
        for title, first, med, lo, hi, n in rows:
            f.write(f"| {title} | {first:.0f} | {med:.0f} | {lo:.0f} | {hi:.0f} | {n} |\n")
    print(f"\nwritten: {OUT_DIR}/{args.label}.md and results.csv")

    if args.explain:
        explain_all(f"{OUT_DIR}/explain_{args.label}.txt")


if __name__ == "__main__":
    main()
