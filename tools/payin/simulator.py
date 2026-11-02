"""Continuous change simulator for the operational database (core.*).

Every second:
  * ~20 new PENDING transactions are inserted; after 2-10 s each becomes SUCCESS or FAILED
  * ~0.8 SUCCESS transactions are reversed (90% recent ones, 10% up to 7 days old -> old days must be corrected)
  * sometimes a small batch of TEST transactions (card_bin 000000) is inserted and hard-deleted 20-60 s later
Every SIM_MERCHANT_INTERVAL_S seconds (default 60) one merchant changes risk_level AND province.
If the column core.transactions.installments appears (schema change), new rows start filling it.
"""
import heapq
import logging
import math
import os
import random
import signal
import time
from collections import Counter, deque
from datetime import timedelta

import psycopg

from .common import connect, next_province, next_risk, setup_logging

log = logging.getLogger("simulator")

TX_PER_SEC = float(os.getenv("SIM_TX_PER_SEC", "20"))
MERCHANT_INTERVAL_S = float(os.getenv("SIM_MERCHANT_INTERVAL_S", "60"))
REVERSALS_PER_SEC = float(os.getenv("SIM_REVERSALS_PER_SEC", "0.8"))
OLD_REVERSAL_SHARE = float(os.getenv("SIM_OLD_REVERSAL_SHARE", "0.10"))
TEST_BATCH_PROB = float(os.getenv("SIM_TEST_BATCH_PROB", "0.15"))   # chance per second
SUCCESS_SHARE = 0.88
TEST_BIN = "000000"             # marks test transactions so leftovers can be cleaned after a restart
RECOVERY_RESOLUTION_S = 10

CHANNELS = ["POS", "IPG", "QR"]
CHANNEL_WEIGHTS = [0.55, 0.30, 0.15]
BINS = ["603799", "610433", "589463", "627353", "621986", "622106", "639346", "636214", "603770", "589210"]

INSERT_PENDING = """
INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status)
SELECT m, c, b, a, 0, 'PENDING'
FROM unnest(%s::bigint[], %s::text[], %s::text[], %s::bigint[]) AS t(m, c, b, a)
RETURNING transaction_id
"""
INSERT_PENDING_INST = """
INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status, installments)
SELECT m, c, b, a, 0, 'PENDING', i
FROM unnest(%s::bigint[], %s::text[], %s::text[], %s::bigint[], %s::int[]) AS t(m, c, b, a, i)
RETURNING transaction_id
"""
INSERT_TEST = """
INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status)
SELECT m, c, b, a, (a * 0.006)::bigint, 'SUCCESS'
FROM unnest(%s::bigint[], %s::text[], %s::text[], %s::bigint[]) AS t(m, c, b, a)
RETURNING transaction_id
"""
RESOLVE = """
UPDATE core.transactions AS t
SET status = v.status,
    fee_amount = CASE WHEN v.status = 'SUCCESS' THEN (t.amount * 0.006)::bigint ELSE 0 END,
    updated_at = now()
FROM unnest(%s::bigint[], %s::text[]) AS v(id, status)
WHERE t.transaction_id = v.id AND t.status = 'PENDING'
"""
REVERSE = """
UPDATE core.transactions SET status = 'REVERSED', updated_at = now()
WHERE transaction_id = ANY(%s) AND status = 'SUCCESS'
"""
DELETE_TESTS = "DELETE FROM core.transactions WHERE transaction_id = ANY(%s)"
RECOVER_PENDING = """
SELECT transaction_id, created_at, now() AS db_now
FROM core.transactions
WHERE status = 'PENDING'
"""


def new_txn_rows(n, n_merchants, rng):
    """Random attributes for n new transactions. Merchant popularity is skewed (a few merchants are big)."""
    merchants = [min(n_merchants, 1 + int(n_merchants * rng.random() ** 2.0)) for _ in range(n)]
    channels = rng.choices(CHANNELS, CHANNEL_WEIGHTS, k=n)
    bins = [rng.choice(BINS) for _ in range(n)]
    amounts = [int(round(50000 * math.exp(rng.random() * 5), -3)) for _ in range(n)]
    installments = [rng.choice([1, 1, 1, 3, 6, 12]) for _ in range(n)]   # never NULL: safe for NOT NULL DEFAULT 1
    return merchants, channels, bins, amounts, installments


def draw_count(rate, rng):
    """Whole number of events for a per-second rate (0.8 -> usually 1 or 0, 2.3 -> 2 or 3)."""
    whole = int(rate)
    return whole + (1 if rng.random() < rate - whole else 0)


def recovered_due(created_at, db_now, monotonic_now):
    """Schedule an orphaned PENDING row for its 10-second resolution deadline."""
    remaining = max(0.0, (created_at + timedelta(seconds=RECOVERY_RESOLUTION_S) - db_now).total_seconds())
    return monotonic_now + remaining


class Simulator:
    def __init__(self):
        self.rng = random.Random()
        self.conn = connect()
        self.stop = False
        self.pending = []                      # heap of (due, txn_id, final_status)
        self.tests = []                        # heap of (due, txn_id)
        self.recent_success = deque(maxlen=20000)
        self.has_installments = False
        self.stats = Counter()
        now = time.monotonic()
        self.next_merchant_change = now + MERCHANT_INTERVAL_S
        self.next_schema_check = now
        self.next_report = now + 10
        self.n_merchants = self.conn.execute("SELECT max(merchant_id) FROM core.merchants").fetchone()[0]
        lo, hi = self.conn.execute(
            "SELECT min(transaction_id), max(transaction_id) FROM core.transactions "
            "WHERE created_at >= now() - interval '7 days'").fetchone()
        self.old_lo, self.old_hi = lo, hi      # pool for "older" reversals
        self.recover()
        log.info("started: %.0f tx/s, merchant change every %.0fs, %d merchants, old-reversal id pool %s..%s",
                 TX_PER_SEC, MERCHANT_INTERVAL_S, self.n_merchants, lo, hi)

    # ---- startup clean-up (after a stop/start the in-memory schedule is gone) ----
    def recover(self):
        rows = self.conn.execute(RECOVER_PENDING).fetchall()
        now = time.monotonic()
        for tid, created_at, db_now in rows:
            final = "SUCCESS" if self.rng.random() < SUCCESS_SHARE else "FAILED"
            heapq.heappush(self.pending, (recovered_due(created_at, db_now, now), tid, final))
        d = self.conn.execute("DELETE FROM core.transactions WHERE card_bin = %s", (TEST_BIN,)).rowcount
        if rows or d:
            log.info("recovery: rescheduled %d PENDING, deleted %d leftover test transactions", len(rows), d)

    # ---- one tick = one second of activity ----
    def tick(self):
        now = time.monotonic()
        self.insert_pending(now)
        self.resolve_due(now)
        self.reverse_some()
        self.test_transactions(now)
        if now >= self.next_merchant_change:
            self.change_merchant()
            self.next_merchant_change = now + MERCHANT_INTERVAL_S
        if now >= self.next_schema_check:
            self.check_schema()
            self.next_schema_check = now + 30
        if now >= self.next_report:
            log.info("last 10s: %s | pending=%d tests=%d installments=%s",
                     dict(self.stats), len(self.pending), len(self.tests), self.has_installments)
            self.stats.clear()
            self.next_report = now + 10

    def insert_pending(self, now):
        n = max(1, int(self.rng.gauss(TX_PER_SEC, TX_PER_SEC * 0.1)))
        m, c, b, a, inst = new_txn_rows(n, self.n_merchants, self.rng)
        if self.has_installments:
            ids = [r[0] for r in self.conn.execute(INSERT_PENDING_INST, (m, c, b, a, inst)).fetchall()]
        else:
            ids = [r[0] for r in self.conn.execute(INSERT_PENDING, (m, c, b, a)).fetchall()]
        for tid in ids:
            final = "SUCCESS" if self.rng.random() < SUCCESS_SHARE else "FAILED"
            heapq.heappush(self.pending, (now + self.rng.uniform(2, 10), tid, final))
        self.stats["inserted"] += len(ids)

    def resolve_due(self, now):
        ids, statuses = [], []
        while self.pending and self.pending[0][0] <= now:
            _, tid, final = heapq.heappop(self.pending)
            ids.append(tid)
            statuses.append(final)
        if ids:
            self.conn.execute(RESOLVE, (ids, statuses))
            self.stats["resolved"] += len(ids)
            self.recent_success.extend(t for t, s in zip(ids, statuses) if s == "SUCCESS")

    def reverse_some(self):
        picks = []
        for _ in range(draw_count(REVERSALS_PER_SEC, self.rng)):
            old = self.old_lo is not None and self.rng.random() < OLD_REVERSAL_SHARE
            if old:
                picks.append(self.rng.randint(self.old_lo, self.old_hi))
            elif self.recent_success:
                picks.append(self.rng.choice(self.recent_success))
        if picks:
            n = self.conn.execute(REVERSE, (picks,)).rowcount
            self.stats["reversed"] += n

    def test_transactions(self, now):
        if self.rng.random() < TEST_BATCH_PROB:
            n = self.rng.randint(2, 3)
            m, c, _, _, _ = new_txn_rows(n, self.n_merchants, self.rng)
            ids = [r[0] for r in self.conn.execute(
                INSERT_TEST, (m, c, [TEST_BIN] * n, [10000] * n)).fetchall()]
            for tid in ids:
                heapq.heappush(self.tests, (now + self.rng.uniform(20, 60), tid))
            self.stats["test_inserted"] += n
        due = []
        while self.tests and self.tests[0][0] <= now:
            due.append(heapq.heappop(self.tests)[1])
        if due:
            self.conn.execute(DELETE_TESTS, (due,))
            self.stats["test_deleted"] += len(due)

    def change_merchant(self):
        mid = self.rng.randint(1, self.n_merchants)
        row = self.conn.execute("SELECT risk_level, province FROM core.merchants WHERE merchant_id = %s",
                                (mid,)).fetchone()
        if row is None:
            return
        risk, province = row
        new_risk, new_prov = next_risk(risk, self.rng), next_province(province, self.rng)
        self.conn.execute(
            "UPDATE core.merchants SET risk_level = %s, province = %s, updated_at = now() WHERE merchant_id = %s",
            (new_risk, new_prov, mid))
        self.stats["merchant_changes"] += 1
        log.info("merchant %d: risk %s -> %s, province %s -> %s", mid, risk, new_risk, province, new_prov)

    def check_schema(self):
        found = self.conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'core' AND table_name = 'transactions' AND column_name = 'installments'"
        ).fetchone() is not None
        if found and not self.has_installments:
            log.info("schema change detected: core.transactions.installments exists, filling it from now on")
        self.has_installments = found

    # ---- main loop ----
    def run(self):
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))
        next_tick = time.monotonic()
        while not self.stop:
            try:
                self.tick()
            except psycopg.Error as exc:
                log.error("database error: %s; reconnecting in 3s", exc)
                time.sleep(3)
                self.conn = connect()
            next_tick += 1.0
            delay = next_tick - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                next_tick = time.monotonic()      # fell behind: do not try to catch up with a burst
        log.info("stopped")


def main():
    setup_logging()
    Simulator().run()


if __name__ == "__main__":
    main()
