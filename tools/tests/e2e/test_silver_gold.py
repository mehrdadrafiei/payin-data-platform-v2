"""End-to-end tests of the transformation plane (silver + gold) against the live stack:   make test-e2e
Each test says what it proves. Timeouts are generous because the transformer runs every ~30 seconds.
"""
import hashlib
import hmac
import re
import time

from .helpers import ch_rows, ch_value, eventually

NEW_TXN = ("INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status) "
           "VALUES (%s, 'POS', '603799', %s, 0, 'PENDING') RETURNING transaction_id")


def fact(tid):
    return ch_rows(f"SELECT status, province_at_txn, risk_level_at_txn, merchant_version_found, toString(created_at) AS ca "
                   f"FROM gold.fct_transactions WHERE transaction_id = {int(tid)}")


def silver(tid):
    return ch_rows(f"SELECT status FROM silver.transactions_current WHERE transaction_id = {int(tid)}")


# ----------------------------------------------------------------- health ----
def test_transformer_runs_are_healthy(pipeline_ready):
    """The last `dbt run` finished successfully and recently."""
    row = ch_rows("SELECT ok, dateDiff('second', started_at, now()) AS age_s FROM ops.dbt_runs "
                  "WHERE kind = 'run' ORDER BY started_at DESC LIMIT 1")[0]
    assert row["ok"] == 1 and row["age_s"] < 300, row


# ----------------------------------------------------------------- silver ----
def test_event_log_has_every_event_exactly_once(pipeline_ready):
    """Duplicates (Debezium restarts, overlapping reads) are removed: one row per (key, LSN, op)."""
    for table in ("transactions", "merchants"):
        dup = ch_value(f"SELECT count() - uniqExact(event_key) FROM silver.{table}_events")
        assert dup == 0, f"{dup} duplicate events in silver.{table}_events"


def test_changes_flow_through_silver_and_gold(pg, pipeline_ready):
    """insert -> update -> hard delete in Postgres show up in the current-state view and in the fact, and the
    delete removes the row from both while the event history keeps all three events."""
    tid = pg.execute(NEW_TXN, (1, 730_000_000 + int(time.time()) % 1_000_000)).fetchone()[0]
    eventually(lambda: silver(tid) == [{"status": "PENDING"}] and len(fact(tid)) == 1, timeout=300,
               what="new transaction in silver.transactions_current and gold.fct_transactions")
    pg.execute("UPDATE core.transactions SET status = 'SUCCESS', fee_amount = 4200, updated_at = now() "
               "WHERE transaction_id = %s", (tid,))
    eventually(lambda: silver(tid) == [{"status": "SUCCESS"}] and fact(tid)[0]["status"] == "SUCCESS", timeout=300,
               what="update visible in silver and gold")
    pg.execute("DELETE FROM core.transactions WHERE transaction_id = %s", (tid,))
    eventually(lambda: silver(tid) == [] and fact(tid) == [], timeout=300, what="delete hides the row in silver and gold")
    ops = [r["op"] for r in ch_rows(f"SELECT op FROM silver.transactions_events WHERE transaction_id = {tid} ORDER BY kafka_offset")]
    assert ops == ["c", "u", "d"], ops


def test_merchant_current_state_equals_postgres(pg, pipeline_ready):
    """Every merchant's current risk level and province in silver equals the operational database."""
    def same():
        src = {r[0]: (r[1], r[2]) for r in pg.execute("SELECT merchant_id, risk_level, province FROM core.merchants")}
        dst = {r["merchant_id"]: (r["risk_level"], r["province"]) for r in
               ch_rows("SELECT merchant_id, risk_level, province FROM silver.merchants_current")}
        return src == dst
    eventually(same, timeout=240, what="silver.merchants_current == core.merchants")


def test_old_transactions_follow_the_source(pg, pipeline_ready):
    """200 old transactions: status and amount in silver equal Postgres (simulator reversals are corrected)."""
    ids = [r[0] for r in pg.execute("SELECT transaction_id FROM core.transactions "
                                    "WHERE transaction_id % 25000 = 7 ORDER BY transaction_id LIMIT 200")]
    assert len(ids) >= 100

    def same():
        src = {r[0]: (r[1], r[2]) for r in pg.execute(
            "SELECT transaction_id, status, amount FROM core.transactions WHERE transaction_id = ANY(%s)", (ids,))}
        dst = {r["transaction_id"]: (r["status"], r["amount"]) for r in ch_rows(
            f"SELECT transaction_id, status, amount FROM silver.transactions_current "
            f"WHERE transaction_id IN ({','.join(map(str, ids))})")}
        return src == dst
    eventually(same, timeout=240, what="silver status/amount == Postgres for the sample")


# ------------------------------------------------------------ PII handling ----
def test_owner_identity_is_only_an_hmac_token(pg, pipeline_ready):
    """Tokens equal HMAC-SHA256(secret key, value) computed independently in Python; the same owner always gets
    the same token; the mobile number is masked; no column is called national_id / mobile in silver or gold."""
    key = open("/secrets/pii_hmac_key").read().strip().encode()
    src = pg.execute("SELECT merchant_id, owner_national_id, owner_mobile FROM core.merchants "
                     "WHERE merchant_id <= 25 OR merchant_id IN (1501, 1502) ORDER BY merchant_id").fetchall()
    dst = {r["merchant_id"]: r for r in ch_rows(
        "SELECT merchant_id, owner_national_id_token AS nid, owner_mobile_token AS mob, owner_mobile_masked AS masked "
        "FROM silver.merchants_current WHERE merchant_id <= 25 OR merchant_id IN (1501, 1502)")}
    for mid, nid, mobile in src:
        assert dst[mid]["nid"] == hmac.new(key, nid.encode(), hashlib.sha256).hexdigest()
        assert dst[mid]["mob"] == hmac.new(key, mobile.encode(), hashlib.sha256).hexdigest()
        assert dst[mid]["masked"] == mobile[:4] + "*****" + mobile[9:]
        assert nid not in dst[mid].values() and mobile not in dst[mid].values()
    assert dst[1]["nid"] == dst[1501]["nid"], "merchants 1 and 1501 share an owner and must share the token"
    cols = [r["name"] for r in ch_rows("SELECT name FROM system.columns WHERE database IN ('silver', 'gold')")]
    bad = [c for c in cols if re.search(r"national_id|mobile", c) and not re.search(r"token|masked", c)]
    assert not bad, f"columns that could hold personal data in clear text: {bad}"
    gold_tokens = [r["name"] for r in ch_rows(
        "SELECT name FROM system.columns WHERE database = 'gold' AND name LIKE '%token%'")]
    assert not gold_tokens, f"gold must not carry pseudonymised identifiers: {gold_tokens}"


# ------------------------------------------------------- SCD2 / gold model ----
def test_current_dimension_version_equals_postgres(pg, pipeline_ready):
    def same():
        src = {r[0]: (r[1], r[2]) for r in pg.execute("SELECT merchant_id, risk_level, province FROM core.merchants")}
        dst = {r["merchant_id"]: (r["risk_level"], r["province"]) for r in
               ch_rows("SELECT merchant_id, risk_level, province FROM gold.dim_merchant WHERE is_current = 1")}
        return src == dst
    eventually(same, timeout=240, what="current dim_merchant versions == core.merchants")


def test_facts_carry_the_merchant_version_valid_at_transaction_time(pipeline_ready):
    """For merchants with several versions, 60 transactions each: the province / risk level in the fact must be the
    version whose validity interval contains created_at (interval logic done independently in Python)."""
    merchants = ch_rows("SELECT merchant_id FROM gold.dim_merchant GROUP BY merchant_id HAVING count() >= 3 "
                        "ORDER BY merchant_id LIMIT 8")
    assert merchants, "no merchant has 3+ versions: was the synthetic history applied?"
    checked = 0
    for m in merchants:
        mid = m["merchant_id"]
        versions = ch_rows(f"SELECT toString(valid_from) AS vf, toString(valid_to) AS vt, province, risk_level "
                           f"FROM gold.dim_merchant WHERE merchant_id = {mid} ORDER BY valid_from")
        facts = ch_rows(f"SELECT toString(created_at) AS ca, province_at_txn, risk_level_at_txn "
                        f"FROM gold.fct_transactions WHERE merchant_id = {mid} ORDER BY cityHash64(transaction_id) LIMIT 60")
        for f in facts:
            v = next(v for v in versions if v["vf"] <= f["ca"] < v["vt"])
            assert (f["province_at_txn"], f["risk_level_at_txn"]) == (v["province"], v["risk_level"]), (mid, f, v)
            checked += 1
    assert checked >= 100, checked


def test_history_is_visible_in_the_fact(pipeline_ready):
    """Point-in-time differs from 'current': some transactions belong to a merchant version that is no longer current."""
    n = ch_value("""SELECT count() FROM gold.fct_transactions AS f
                    INNER JOIN (SELECT merchant_id, risk_level FROM gold.dim_merchant WHERE is_current = 1) AS d
                    ON f.merchant_id = d.merchant_id
                    WHERE f.risk_level_at_txn != d.risk_level""")
    assert n > 0


def test_dimension_change_corrects_existing_facts(pg, pipeline_ready):
    """A merchant change that is dated BEFORE an existing transaction must re-assign that transaction (the
    merchant event can arrive after the transactions it applies to)."""
    mid = 1999
    tid = pg.execute(NEW_TXN, (mid, 740_000_000 + int(time.time()) % 1_000_000)).fetchone()[0]
    try:
        eventually(lambda: len(fact(tid)) == 1, timeout=300, what="transaction in the fact")
        before = fact(tid)[0]
        current = pg.execute("SELECT risk_level FROM core.merchants WHERE merchant_id = %s", (mid,)).fetchone()[0]
        new_risk = next(r for r in ("LOW", "MEDIUM", "HIGH") if r != before["risk_level_at_txn"] and r != current)
        last_from = ch_value(f"SELECT toString(max(valid_from)) FROM gold.dim_merchant WHERE merchant_id = {mid}")
        # the change must start after the merchant's latest version, otherwise it is not a valid new version
        pg.execute("UPDATE core.merchants SET risk_level = %s, "
                   "updated_at = (SELECT created_at FROM core.transactions WHERE transaction_id = %s) - interval '1 minute' "
                   "WHERE merchant_id = %s", (new_risk, tid, mid))
        eventually(lambda: fact(tid) and fact(tid)[0]["risk_level_at_txn"] == new_risk, timeout=300,
                   what=f"transaction {tid} re-assigned to risk level {new_risk} (was {before['risk_level_at_txn']}; "
                        f"merchant's previous latest version started {last_from})")
    finally:
        pg.execute("DELETE FROM core.transactions WHERE transaction_id = %s", (tid,))


# -------------------------------------------------------------- aggregates ----
def test_daily_channel_aggregate_equals_the_fact(pipeline_ready):
    cols = ("total_count, pending_count, success_count, failed_count, reversed_count, success_amount")

    def same():
        agg = ch_rows(f"SELECT toString(txn_date) AS d, channel, {cols} FROM gold.agg_daily_channel "
                      f"WHERE txn_date >= today() - 7 ORDER BY d, channel")
        raw = ch_rows("SELECT toString(txn_date) AS d, channel, count() AS total_count, "
                      "countIf(status = 'PENDING') AS pending_count, countIf(status = 'SUCCESS') AS success_count, "
                      "countIf(status = 'FAILED') AS failed_count, countIf(status = 'REVERSED') AS reversed_count, "
                      "sumIf(amount, status = 'SUCCESS') AS success_amount FROM gold.fct_transactions "
                      "WHERE txn_date >= today() - 7 GROUP BY txn_date, channel ORDER BY d, channel")
        return agg == raw
    eventually(same, timeout=240, what="agg_daily_channel == aggregation of the fact (last 7 days)")
