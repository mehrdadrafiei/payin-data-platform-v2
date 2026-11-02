"""The five dashboard cards, defined once. Day = Asia/Tehran calendar day, month = Gregorian month in Asia/Tehran.

core_sql  : the BASELINE queries, run directly on the operational Postgres (core.*), as the brief describes.
gold_sql  : the OPTIMIZED queries on the analytical platform (added in a later phase).
Metrics: success = status SUCCESS. Success rate = SUCCESS / (SUCCESS + FAILED + REVERSED), PENDING excluded.
"""
TZ = "Asia/Tehran"


def window_start(days: int) -> str:
    """Tehran midnight of (today - days + 1), so the window is `days` calendar days including today.
    Written as a plain timestamptz bound so the created_at index can be used."""
    return f"(((now() AT TIME ZONE '{TZ}')::date - {days - 1})::timestamp AT TIME ZONE '{TZ}')"


MONTH_START = f"(date_trunc('month', now() AT TIME ZONE '{TZ}') AT TIME ZONE '{TZ}')"

CARDS = [
    {
        "key": "card1",
        "title": "1. Daily successful amount and count (last 90 days)",
        "display": "line",
        "viz": {"graph.dimensions": ["day"], "graph.metrics": ["success_amount", "success_count"]},
        "core_sql": f"""SELECT (t.created_at AT TIME ZONE '{TZ}')::date AS day,
       count(*) AS success_count,
       sum(t.amount) AS success_amount
FROM core.transactions t
WHERE t.status = 'SUCCESS'
  AND t.created_at >= {window_start(90)}
GROUP BY 1
ORDER BY 1""",
    },
    {
        "key": "card2",
        "title": "2. Daily success rate by channel (last 30 days)",
        "display": "line",
        "viz": {"graph.dimensions": ["day", "channel"], "graph.metrics": ["success_rate_pct"]},
        "core_sql": f"""SELECT (t.created_at AT TIME ZONE '{TZ}')::date AS day,
       t.channel,
       round(100.0 * count(*) FILTER (WHERE t.status = 'SUCCESS')
             / NULLIF(count(*) FILTER (WHERE t.status <> 'PENDING'), 0), 2) AS success_rate_pct
FROM core.transactions t
WHERE t.created_at >= {window_start(30)}
GROUP BY 1, 2
ORDER BY 1, 2""",
    },
    {
        "key": "card3",
        "title": "3. Top 10 merchants this month by successful amount",
        "display": "bar",
        "viz": {"graph.dimensions": ["name"], "graph.metrics": ["success_amount"]},
        "core_sql": f"""SELECT m.merchant_id,
       m.name,
       sum(t.amount) AS success_amount,
       count(*) AS success_count
FROM core.transactions t
JOIN core.merchants m USING (merchant_id)
WHERE t.status = 'SUCCESS'
  AND t.created_at >= {MONTH_START}
GROUP BY m.merchant_id, m.name
ORDER BY success_amount DESC
LIMIT 10""",
    },
    {
        "key": "card4",
        "title": "4. Fees by merchant category (MCC) and province (last 90 days)",
        "display": "table",
        "viz": {},
        "core_sql": f"""SELECT m.mcc,
       m.province,
       sum(t.fee_amount) AS fee_amount
FROM core.transactions t
JOIN core.merchants m USING (merchant_id)
WHERE t.status = 'SUCCESS'
  AND t.created_at >= {window_start(90)}
GROUP BY m.mcc, m.province
ORDER BY m.mcc, m.province""",
    },
    {
        "key": "card5",
        "title": "5. Monthly successful volume by merchant risk level",
        "display": "bar",
        "viz": {"graph.dimensions": ["month", "risk_level"], "graph.metrics": ["success_amount"],
                "stackable.stack_type": "stacked"},
        "core_sql": f"""SELECT date_trunc('month', t.created_at AT TIME ZONE '{TZ}')::date AS month,
       m.risk_level,
       count(*) AS success_count,
       sum(t.amount) AS success_amount
FROM core.transactions t
JOIN core.merchants m USING (merchant_id)
WHERE t.status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY 1, 2""",
    },
]

BASELINE_PREFIX = "[core] "
BASELINE_DASHBOARD = "Daily Sales / \u0641\u0631\u0648\u0634 \u0631\u0648\u0632\u0627\u0646\u0647 \u2014 baseline (core)"


# ---- optimized (gold) queries: same output columns as the baseline, so the same viz settings work ----
TODAY_T = f"toDate(toTimeZone(now(), '{TZ}'))"

GOLD_SQL = {
    "card1": f"""SELECT day, cnt AS success_count, amt AS success_amount
FROM (
    SELECT txn_date AS day, sum(success_count) AS cnt, sum(success_amount) AS amt
    FROM gold.agg_daily_channel
    WHERE txn_date >= {TODAY_T} - 89
    GROUP BY txn_date
    HAVING cnt > 0
)
ORDER BY day""",
    "card2": f"""SELECT txn_date AS day,
       channel,
       round(100.0 * success_count / nullIf(success_count + failed_count + reversed_count, 0), 2) AS success_rate_pct
FROM gold.agg_daily_channel
WHERE txn_date >= {TODAY_T} - 29
ORDER BY day, channel""",
    "card3": f"""SELECT a.merchant_id AS merchant_id,
       d.name AS name,
       a.success_amount AS success_amount,
       a.success_count AS success_count
FROM gold.agg_merchant_month AS a
INNER JOIN (SELECT merchant_id, name FROM gold.dim_merchant WHERE is_current = 1) AS d
        ON a.merchant_id = d.merchant_id
WHERE a.txn_month = toStartOfMonth({TODAY_T})
ORDER BY success_amount DESC
LIMIT 10""",
    "card4": f"""SELECT mcc, province_at_txn AS province, sum(success_fee_amount) AS fee_amount
FROM gold.agg_fee_day_mcc_province
WHERE txn_date >= {TODAY_T} - 89
GROUP BY mcc, province_at_txn
ORDER BY mcc, province""",
    "card5": """SELECT txn_month AS month, risk_level_at_txn AS risk_level, success_count, success_amount
FROM gold.agg_month_risk
ORDER BY month, risk_level""",
}
for _c in CARDS:
    _c["gold_sql"] = GOLD_SQL[_c["key"]]

OPTIMIZED_PREFIX = "[gold] "
OPTIMIZED_DASHBOARD = "Daily Sales / \u0641\u0631\u0648\u0634 \u0631\u0648\u0632\u0627\u0646\u0647 \u2014 optimized (gold)"