"""End-to-end proof that CDC (Postgres -> Debezium -> Kafka -> ClickHouse bronze) is correct.

Run against the live stack:   make test-e2e
The simulator may keep running; every test works with its own rows.
"""
import time

from .helpers import ch_rows, ch_value, connector_status, eventually, events_for

NEW_TXN = ("INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status) "
           "VALUES (1, 'POS', '603799', %s, 0, 'PENDING') RETURNING transaction_id")


def ops(table, pk):
    return [e["op"] for e in events_for(table, pk)]


# ---------------------------------------------------------------- health ----
def test_connector_and_task_are_running():
    st = connector_status()
    assert st["connector"]["state"] == "RUNNING"
    assert st["tasks"] and all(t["state"] == "RUNNING" for t in st["tasks"])


def test_no_broken_messages():
    assert ch_value("SELECT count() FROM bronze.cdc_errors") == 0


# ------------------------------------------------------------- snapshot -----
def test_initial_snapshot_is_complete_and_has_no_duplicates(pg):
    """Every seeded row must have arrived exactly once as a snapshot ('r') event.
    On a fresh start Kafka may still be delivering millions of events, so wait until the counts settle."""
    seed_ts = pg.execute("SELECT value::timestamptz FROM public._seed_state WHERE key = 'seed_ts'").fetchone()[0]
    n_txn = pg.execute("SELECT count(*) FROM core.transactions WHERE created_at <= %s", (seed_ts,)).fetchone()[0]
    n_mer = pg.execute("SELECT count(*) FROM core.merchants").fetchone()[0]

    def snapshot_counts():
        r_txn = ch_rows("SELECT count() AS n, uniqExact(pk) AS keys FROM bronze.transactions_cdc WHERE op = 'r'")[0]
        r_mer = ch_rows("SELECT count() AS n, uniqExact(pk) AS keys FROM bronze.merchants_cdc WHERE op = 'r'")[0]
        return r_txn, r_mer

    def complete():
        r_txn, r_mer = snapshot_counts()
        return r_txn["n"] == r_txn["keys"] == n_txn and r_mer["n"] == r_mer["keys"] == n_mer

    try:
        eventually(complete, timeout=900, interval=10,
                   what="complete snapshot in bronze (no missing rows, no duplicates)")
    except AssertionError:
        r_txn, r_mer = snapshot_counts()
        assert r_txn["n"] == r_txn["keys"] == n_txn, f"transactions: expected {n_txn}, got {r_txn}"
        assert r_mer["n"] == r_mer["keys"] == n_mer, f"merchants: expected {n_mer}, got {r_mer}"
        raise


# ------------------------------------------------ insert / update / delete --
def test_insert_update_delete_roundtrip_keeps_order(pg):
    t0 = time.time()
    tid = pg.execute(NEW_TXN, (700_000_000 + int(t0) % 1_000_000,)).fetchone()[0]
    eventually(lambda: ops("transactions", tid) == ["c"], what="insert event (c)")
    latency = time.time() - t0

    pg.execute("UPDATE core.transactions SET status = 'SUCCESS', fee_amount = 4200, updated_at = now() "
               "WHERE transaction_id = %s", (tid,))
    eventually(lambda: ops("transactions", tid) == ["c", "u"], what="update event (u)")

    pg.execute("DELETE FROM core.transactions WHERE transaction_id = %s", (tid,))
    eventually(lambda: ops("transactions", tid) == ["c", "u", "d"], what="delete event (d)")

    ev = events_for("transactions", tid)
    # order is preserved: Kafka offsets AND Postgres log positions both strictly increase
    assert [e["_offset"] for e in ev] == sorted(e["_offset"] for e in ev)
    lsns = [e["source_lsn"] for e in ev]
    assert lsns == sorted(set(lsns)), f"LSNs must be distinct and increasing: {lsns}"
    # content of each version
    assert ev[0]["doc"]["after"]["status"] == "PENDING"
    assert ev[1]["doc"]["after"]["status"] == "SUCCESS" and ev[1]["doc"]["after"]["fee_amount"] == 4200
    # a delete carries only the key: 'after' is null and the other 'before' fields are placeholders
    assert ev[2]["doc"]["after"] is None
    assert ev[2]["doc"]["before"]["transaction_id"] == tid
    assert ev[2]["doc"]["before"]["created_at"].startswith("1970"), "placeholder values expected on delete"
    print(f"\nend-to-end latency insert -> bronze: {latency:.1f}s")
    assert latency < 60


def test_changes_of_one_postgres_transaction_get_distinct_increasing_lsns(pg):
    ids = [pg.execute(NEW_TXN, (710_000_000 + i,)).fetchone()[0] for i in range(3)]
    eventually(lambda: all(ops("transactions", i) == ["c"] for i in ids), what="3 insert events")
    with pg.transaction():                                  # ONE transaction, three rows
        pg.execute("UPDATE core.transactions SET status = 'FAILED', updated_at = now() "
                   "WHERE transaction_id = ANY(%s)", (ids,))
    eventually(lambda: all(ops("transactions", i) == ["c", "u"] for i in ids), what="3 update events")
    lsns = [events_for("transactions", i)[1]["source_lsn"] for i in ids]
    assert len(set(lsns)) == 3
    pg.execute("DELETE FROM core.transactions WHERE transaction_id = ANY(%s)", (ids,))


def test_merchant_update_that_changes_nothing_still_produces_an_event(pg):
    """Merchant 2000: only updated_at moves. CDC must report it; the SCD2 logic later must NOT create a version."""
    before = len(events_for("merchants", 2000))
    pg.execute("UPDATE core.merchants SET updated_at = now() WHERE merchant_id = 2000")
    eventually(lambda: len(events_for("merchants", 2000)) == before + 1, what="merchant update event")
    last = events_for("merchants", 2000)[-1]
    assert last["op"] == "u" and last["doc"]["after"]["merchant_id"] == 2000


# ------------------------------------------------------ schema evolution ----
def test_adding_a_column_does_not_stop_the_pipeline(pg, pipeline_ready):
    """Add `installments` (no-op if it already exists). The connector must keep running and the new
    column must show up in the stored event payload without any manual step."""
    pg.execute("ALTER TABLE core.transactions ADD COLUMN IF NOT EXISTS installments INTEGER")
    errors_before = ch_value("SELECT count() FROM bronze.cdc_errors")

    tid = pg.execute("INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status, installments) "
                     "VALUES (1, 'IPG', '603799', 720000000, 0, 'PENDING', 7) RETURNING transaction_id").fetchone()[0]
    eventually(lambda: ops("transactions", tid) == ["c"], timeout=120, what="insert event after ALTER TABLE")
    assert events_for("transactions", tid)[0]["doc"]["after"]["installments"] == 7

    # a row inserted the old way (column omitted) keeps flowing as well
    tid2 = pg.execute(NEW_TXN, (720_000_001,)).fetchone()[0]
    eventually(lambda: ops("transactions", tid2) == ["c"], what="insert event without the new column")

    def installments_reached_models():
        silver = ch_rows(f"SELECT installments FROM silver.transactions_current WHERE transaction_id = {tid}")
        gold = ch_rows(f"SELECT installments FROM gold.fct_transactions WHERE transaction_id = {tid}")
        return silver == [{"installments": 7}] and gold == [{"installments": 7}]

    eventually(installments_reached_models, timeout=300,
               what="installments value in silver current state and gold fact")
    assert connector_status()["connector"]["state"] == "RUNNING"
    assert ch_value("SELECT count() FROM bronze.cdc_errors") == errors_before
    assert pg.execute("SELECT 1").fetchone()[0] == 1
    pg.execute("DELETE FROM core.transactions WHERE transaction_id = ANY(%s)", ([tid, tid2],))


# ------------------------------------------------------- global invariants --
def test_per_key_order_is_consistent_in_bronze():
    """For every key, events sorted by Kafka offset must also be sorted by LSN (so LSN works as a version)."""
    for table in ("transactions", "merchants"):
        bad = ch_value(f"""
            SELECT count() FROM (
                SELECT pk,
                       arrayMap(x -> x.2, arraySort(groupArray((_offset, source_lsn)))) AS lsns
                FROM bronze.{table}_cdc
                WHERE pk IN (SELECT pk FROM bronze.{table}_cdc WHERE op != 'r')
                GROUP BY pk
            ) WHERE lsns != arraySort(lsns)""")
        assert bad == 0, f"{bad} keys of {table} have events whose LSN order differs from offset order"


def test_no_duplicate_events_in_a_healthy_run():
    """Expected 0 unless the connector crashed and re-sent events (at-least-once). Silver removes duplicates anyway."""
    for table in ("transactions", "merchants"):
        dups = ch_value(f"SELECT count() FROM (SELECT pk, source_lsn, op FROM bronze.{table}_cdc "
                        f"WHERE op != 'r' GROUP BY pk, source_lsn, op HAVING count() > 1)")
        assert dups == 0, f"{dups} duplicated event identities in {table}"
