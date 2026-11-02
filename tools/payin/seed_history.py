"""Synthetic merchant history.

The base seed creates every merchant in one initial state (dated 181 days ago). CDC only sees changes made
AFTER it starts, so to get a meaningful 180-day history of risk_level / province we replay ~3,000 changes
with BACKDATED updated_at values, in chronological order, once the replication slot exists.

All of this is synthetic (see docs/synthetic-data.md). Commit time is "now", business time (updated_at) is in
the past; real CDC streams never look like that, so lag metrics must use commit time, never valid_from.
"""
import logging
import os
import random
import time
from datetime import timedelta

from .common import connect, next_province, next_risk, setup_logging

log = logging.getLogger("seed_history")

SLOT = os.getenv("CDC_SLOT_NAME", "payin_slot")
HISTORY_DAYS = 175            # changes happen between (seed time - 175 days) and (now - 12 hours)
END_MARGIN_HOURS = 12
CHANGE_COUNTS = [0, 1, 2, 3]  # changes per merchant ...
CHANGE_WEIGHTS = [0.25, 0.25, 0.25, 0.25]  # ... and how likely each count is
RISK_CHANGE_SHARE = 0.85      # otherwise the province changes


def plan_history(merchant_ids, start, end, rng):
    """Return [(timestamp, merchant_id)] sorted chronologically."""
    span = (end - start).total_seconds()
    events = []
    for mid in merchant_ids:
        for _ in range(rng.choices(CHANGE_COUNTS, CHANGE_WEIGHTS)[0]):
            events.append((start + timedelta(seconds=span * rng.random()), mid))
    events.sort()
    return events


def wait_for_slot(conn, timeout_s: int = 900) -> None:
    waited = 0
    while True:
        row = conn.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone()
        if row:
            return
        if waited >= timeout_s:
            raise TimeoutError(f"replication slot {SLOT} did not appear within {timeout_s}s")
        if waited % 15 == 0:
            log.info("waiting for replication slot %s (Debezium not started yet)...", SLOT)
        time.sleep(3)
        waited += 3


def main() -> None:
    setup_logging()
    conn = connect()
    if conn.execute("SELECT 1 FROM public._seed_state WHERE key = 'history_done'").fetchone():
        log.info("history already applied, skipping")
        return

    seed_ts = conn.execute("SELECT value::timestamptz FROM public._seed_state WHERE key = 'seed_ts'").fetchone()[0]
    now = conn.execute("SELECT now()").fetchone()[0]
    start = seed_ts - timedelta(days=HISTORY_DAYS)
    end = now - timedelta(hours=END_MARGIN_HOURS)

    wait_for_slot(conn)

    rng = random.Random(42)
    ids = [r[0] for r in conn.execute("SELECT merchant_id FROM core.merchants ORDER BY 1")]
    events = plan_history(ids, start, end, rng)
    log.info("applying %d backdated merchant changes between %s and %s", len(events), start, end)

    risk_changes = province_changes = 0
    for ts, mid in events:
        risk, province = conn.execute(
            "SELECT risk_level, province FROM core.merchants WHERE merchant_id = %s", (mid,)).fetchone()
        if rng.random() < RISK_CHANGE_SHARE:
            conn.execute("UPDATE core.merchants SET risk_level = %s, updated_at = %s WHERE merchant_id = %s",
                         (next_risk(risk, rng), ts, mid))
            risk_changes += 1
        else:
            conn.execute("UPDATE core.merchants SET province = %s, updated_at = %s WHERE merchant_id = %s",
                         (next_province(province, rng), ts, mid))
            province_changes += 1

    conn.execute("INSERT INTO public._seed_state (key, value) VALUES ('history_done', %s)", (str(len(events)),))
    log.info("done: %d risk changes, %d province changes", risk_changes, province_changes)


if __name__ == "__main__":
    main()
