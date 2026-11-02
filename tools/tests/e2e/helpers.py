"""Helpers for the end-to-end tests: ClickHouse over HTTP, Debezium REST, polling."""
import json
import os
import time
import urllib.parse
import urllib.request

CH_URL = os.getenv("CH_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "ch_pw")
CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")


def ch_rows(sql: str):
    """Run a ClickHouse query, return a list of dicts (64-bit integers come back as numbers)."""
    params = urllib.parse.urlencode({"user": CH_USER, "password": CH_PASSWORD,
                                     "default_format": "JSONEachRow",
                                     "output_format_json_quote_64bit_integers": 0})
    req = urllib.request.Request(f"{CH_URL}/?{params}", data=sql.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return [json.loads(line) for line in resp.read().decode().splitlines() if line.strip()]


def ch_value(sql: str):
    rows = ch_rows(sql)
    return next(iter(rows[0].values())) if rows else None


def connector_status():
    with urllib.request.urlopen(f"{CONNECT_URL}/connectors/payin-cdc/status", timeout=30) as resp:
        return json.loads(resp.read())


def eventually(fn, timeout: float = 90, interval: float = 1.0, what: str = "condition"):
    """Call fn() until it returns something truthy; fail with a clear message after `timeout` seconds."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what} (last value: {last!r})")


def events_for(table: str, pk: int):
    """All bronze events of one primary key, oldest first. table = 'transactions' | 'merchants'."""
    rows = ch_rows(f"SELECT op, _offset, source_lsn, source_ts_ms, payload "
                   f"FROM bronze.{table}_cdc WHERE pk = {int(pk)} ORDER BY _offset")
    for r in rows:
        r["doc"] = json.loads(r["payload"])
    return rows


def pipeline_lag():
    """Offsets of bronze vs silver vs gold. Returns None while the tables do not exist yet."""
    try:
        return ch_rows("""SELECT
            (SELECT max(_offset) FROM bronze.transactions_cdc)            AS b_t,
            (SELECT max(kafka_offset) FROM silver.stg_transactions_events) AS e_t,
            (SELECT max(kafka_offset) FROM silver.transactions_state)      AS s_t,
            (SELECT max(src_kafka_offset) FROM gold.fct_transactions_state) AS g_t,
            (SELECT max(_offset) FROM bronze.merchants_cdc)                AS b_m,
            (SELECT max(kafka_offset) FROM silver.stg_merchants_events)    AS e_m,
            (SELECT max(kafka_offset) FROM silver.merchants_state)         AS s_m,
            (SELECT count() FROM gold.dim_merchant)                        AS dim_rows""")[0]
    except Exception:
        return None


def caught_up(tolerance: int = 5000):
    """True when silver and gold are within `tolerance` events of bronze (the simulator keeps adding ~40 events/s)."""
    lag = pipeline_lag()
    if not lag or not lag["dim_rows"]:
        return False
    return (lag["b_t"] - lag["e_t"] <= tolerance and lag["b_t"] - lag["s_t"] <= tolerance
            and lag["b_t"] - lag["g_t"] <= tolerance and lag["b_m"] - lag["e_m"] <= tolerance
            and lag["b_m"] - lag["s_m"] <= tolerance)
