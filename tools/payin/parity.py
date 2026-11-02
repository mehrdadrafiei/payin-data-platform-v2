"""Parity + reconciliation: baseline (core.*, PostgreSQL) vs optimized (gold.*, ClickHouse).

  python -m payin.parity [--wait SECONDS]        (make parity-quiet stops the simulator first)

EXACT checks (must pass):
  R1  transaction count per status: core == silver.transactions_current == gold.fct_transactions
  R2  successful count and amount per Tehran day, ALL history: core == gold.agg_daily_channel
  cards 1-3: baseline result == optimized result (definitions unchanged)
  cards 4-5: the optimized card INTENTIONALLY differs (merchant attributes at transaction time). Proven by
      (a) re-deriving the baseline's current-attribute result from gold.fct_transactions + current dimension
          version: must equal the baseline exactly, and
      (b) totals per MCC (card 4) / per month (card 5) of the optimized card must equal the baseline.
INFORMATIONAL: how much of card 4 / card 5 moves because of point-in-time attributes.

The source must be quiet for exact equality; with --wait the whole comparison is retried until it passes, so the
pipeline can catch up. The report is written to benchmarks/parity.md. Exit code 1 if an exact check fails.
"""
import argparse
import datetime as dt
import decimal
import json
import os
import sys
import time
import urllib.parse
import urllib.request

import psycopg
from psycopg.rows import dict_row

from . import cards as C

CH_URL = os.getenv("CH_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "ch_pw")
OUT_DIR = os.getenv("BENCH_DIR", "/app/benchmarks")
CARD = {c["key"]: c for c in C.CARDS}

CH_CARD4_CURRENT = f"""SELECT mcc, province, s AS fee_amount FROM (
    SELECT d.mcc AS mcc, d.province AS province, sum(f.fee_amount) AS s
    FROM gold.fct_transactions AS f
    INNER JOIN (SELECT merchant_id, mcc, province FROM gold.dim_merchant WHERE is_current = 1) AS d
            ON f.merchant_id = d.merchant_id
    WHERE f.status = 'SUCCESS' AND f.txn_date >= {C.TODAY_T} - 89
    GROUP BY d.mcc, d.province
) ORDER BY mcc, province"""

CH_CARD5_CURRENT = """SELECT f.txn_month AS month, d.risk_level AS risk_level,
       count() AS success_count, sum(f.amount) AS success_amount
FROM gold.fct_transactions AS f
INNER JOIN (SELECT merchant_id, risk_level FROM gold.dim_merchant WHERE is_current = 1) AS d
        ON f.merchant_id = d.merchant_id
WHERE f.status = 'SUCCESS'
GROUP BY f.txn_month, d.risk_level
ORDER BY month, risk_level"""

PG_DAILY = f"""SELECT (created_at AT TIME ZONE '{C.TZ}')::date AS day, count(*) AS success_count, sum(amount) AS success_amount
FROM core.transactions WHERE status = 'SUCCESS' GROUP BY 1 ORDER BY 1"""
CH_DAILY = """SELECT day, cnt AS success_count, amt AS success_amount FROM (
    SELECT txn_date AS day, sum(success_count) AS cnt, sum(success_amount) AS amt
    FROM gold.agg_daily_channel GROUP BY txn_date HAVING cnt > 0) ORDER BY day"""


# ------------------------------------------------------------------ plumbing --
def ch_rows(sql):
    params = urllib.parse.urlencode({"user": CH_USER, "password": CH_PASSWORD, "default_format": "JSONEachRow",
                                     "output_format_json_quote_64bit_integers": 0})
    req = urllib.request.Request(f"{CH_URL}/?{params}", data=sql.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=300) as resp:
        return [json.loads(line) for line in resp.read().decode().splitlines() if line.strip()]


def pg_rows(pg, sql):
    return pg.execute(sql).fetchall()


def norm(v):
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()[:10]
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    return v


def same(x, y, tol):
    if x is None or y is None:
        return x is y
    if isinstance(x, (int, float)) and isinstance(y, (int, float)):
        return abs(x - y) <= tol
    return x == y


def diff(a, b, key, names=("core", "gold"), tol=0.0):
    """Row-level differences between two result sets, matched on the `key` columns."""
    ka = {tuple(norm(r[k]) for k in key): r for r in a}
    kb = {tuple(norm(r[k]) for k in key): r for r in b}
    problems = []
    for k in sorted(set(ka) | set(kb), key=str):
        ra, rb = ka.get(k), kb.get(k)
        if ra is None or rb is None:
            problems.append(f"{k}: only in {names[1] if ra is None else names[0]}")
            continue
        for col in ra:
            if col in key:
                continue
            x, y = norm(ra[col]), norm(rb.get(col))
            if not same(x, y, tol):
                problems.append(f"{k} {col}: {names[0]}={x} {names[1]}={y}")
    return problems


def rollup(rows, key, val):
    acc = {}
    for r in rows:
        k = norm(r[key])
        acc[k] = acc.get(k, 0) + norm(r[val])
    return [{key: k, val: v} for k, v in acc.items()]


def moved_share(a, b, key, val):
    """Percent of the total that sits in a different cell in b than in a."""
    ka = {tuple(norm(r[k]) for k in key): norm(r[val]) for r in a}
    kb = {tuple(norm(r[k]) for k in key): norm(r[val]) for r in b}
    delta = sum(abs(ka.get(c, 0) - kb.get(c, 0)) for c in set(ka) | set(kb))
    return 100.0 * delta / 2 / (sum(ka.values()) or 1)


# -------------------------------------------------------------------- checks --
def run(pg):
    res = []                                    # (name, problems, info)

    def add(name, problems, info=""):
        res.append((name, problems, info))

    core_status = pg_rows(pg, "SELECT status, count(*) AS n FROM core.transactions GROUP BY status")
    add("R1 transactions per status: core == silver == gold",
        [f"silver: {p}" for p in diff(core_status, ch_rows(
            "SELECT status, count() AS n FROM silver.transactions_current GROUP BY status"), ["status"],
            ("core", "silver"))]
        + [f"gold: {p}" for p in diff(core_status, ch_rows(
            "SELECT status, count() AS n FROM gold.fct_transactions GROUP BY status"), ["status"])])
    add("R2 successful count/amount per Tehran day (all history): core == gold",
        diff(pg_rows(pg, PG_DAILY), ch_rows(CH_DAILY), ["day"]))

    for key, keycols, tol in (("card1", ["day"], 0.0), ("card2", ["day", "channel"], 0.011),
                              ("card3", ["merchant_id"], 0.0)):
        c = CARD[key]
        add(f"{c['title']}: baseline == optimized",
            diff(pg_rows(pg, c["core_sql"]), ch_rows(c["gold_sql"]), keycols, tol=tol))

    c4 = CARD["card4"]
    core4, gold4 = pg_rows(pg, c4["core_sql"]), ch_rows(c4["gold_sql"])
    add("card 4: baseline re-derived from gold fact (current province) == baseline",
        diff(core4, ch_rows(CH_CARD4_CURRENT), ["mcc", "province"]))
    add("card 4: fee totals per MCC, optimized == baseline",
        diff(rollup(core4, "mcc", "fee_amount"), rollup(gold4, "mcc", "fee_amount"), ["mcc"]),
        f"INFO: {moved_share(core4, gold4, ['mcc', 'province'], 'fee_amount'):.1f}% of fee amount sits in a "
        f"different province than the merchant's current one (province at transaction time)")

    c5 = CARD["card5"]
    core5, gold5 = pg_rows(pg, c5["core_sql"]), ch_rows(c5["gold_sql"])
    add("card 5: baseline re-derived from gold fact (current risk) == baseline",
        diff(core5, ch_rows(CH_CARD5_CURRENT), ["month", "risk_level"]))
    add("card 5: amount and count per month, optimized == baseline",
        diff(rollup(core5, "month", "success_amount"), rollup(gold5, "month", "success_amount"), ["month"])
        + diff(rollup(core5, "month", "success_count"), rollup(gold5, "month", "success_count"), ["month"]),
        f"INFO: {moved_share(core5, gold5, ['month', 'risk_level'], 'success_amount'):.1f}% of successful amount "
        f"sits in a different risk level than the merchant's current one (risk level at transaction time)")
    return res


def write_report(results, attempts, path):
    stamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    lines = [f"# Parity and reconciliation report\n", f"Run at {stamp} UTC, {attempts} attempt(s). "
             "Exact checks must pass; INFO lines describe intended differences.\n",
             "| Check | Result |", "|---|---|"]
    for name, problems, _ in results:
        lines.append(f"| {name} | {'PASS' if not problems else f'FAIL ({len(problems)} differences)'} |")
    for name, problems, info in results:
        if problems:
            lines += ["", f"### {name}"] + [f"- {p}" for p in problems[:8]]
        if info:
            lines += ["", info]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=int, default=0, help="retry for this many seconds until all checks pass")
    args = ap.parse_args()
    deadline = time.time() + args.wait
    attempts = 0
    with psycopg.connect(autocommit=True, row_factory=dict_row) as pg:
        while True:
            attempts += 1
            results = run(pg)
            failed = [r for r in results if r[1]]
            if not failed or time.time() >= deadline:
                break
            print(f"attempt {attempts}: {len(failed)} check(s) differ; retrying in 15 s (pipeline catching up?)")
            time.sleep(15)
    print(write_report(results, attempts, f"{OUT_DIR}/parity.md"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())