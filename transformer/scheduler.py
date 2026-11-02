"""Transformer: keeps silver (and later gold) up to date.

* writes the PII key pads to ops.pii_pads (key file lives in a Docker volume; created on first start)
* runs `dbt run` every DBT_INTERVAL_S seconds (back to back while it is catching up)
* runs `dbt test` every DBT_TEST_EVERY_S seconds (full-scan checks are too heavy for every cycle)
* logs every run to ops.dbt_runs
Ingestion (Kafka -> bronze) does not depend on this process at all.
"""
import hashlib
import json
import os
import secrets
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request

CH_URL = os.getenv("CH_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "ch_pw")
KEY_FILE = os.getenv("PII_KEY_FILE", "/secrets/pii_hmac_key")
DBT_DIR = os.getenv("DBT_DIR", "/app/dbt")
STATE_DIR = os.getenv("DBT_STATE_DIR", "/tmp/dbt")
INTERVAL_S = float(os.getenv("DBT_INTERVAL_S", "30"))
TEST_EVERY_S = float(os.getenv("DBT_TEST_EVERY_S", "600"))
RUN_TIMEOUT_S = float(os.getenv("DBT_RUN_TIMEOUT_S", "1800"))

stop = False


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def ch(sql, data=None):
    params = urllib.parse.urlencode({"user": CH_USER, "password": CH_PASSWORD})
    body = (sql + ("\n" + data if data else "")).encode()
    req = urllib.request.Request(f"{CH_URL}/?{params}", data=body, method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode()


def load_or_create_key() -> bytes:
    if not os.path.exists(KEY_FILE):
        os.makedirs(os.path.dirname(KEY_FILE), exist_ok=True)
        with open(os.open(KEY_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640), "w") as f:
            f.write(secrets.token_hex(32))
        log("created a new PII HMAC key (stored only in the secrets volume)")
    with open(KEY_FILE) as f:
        return f.read().strip().encode()


def pads(key: bytes):
    """HMAC key blocks: (key xor 0x36...) and (key xor 0x5c...), key zero-padded to the 64-byte SHA-256 block."""
    if len(key) > 64:
        key = hashlib.sha256(key).digest()
    k = key.ljust(64, b"\x00")
    return bytes(b ^ 0x36 for b in k).hex(), bytes(b ^ 0x5C for b in k).hex()


def setup_ops(key: bytes):
    for db in ("bronze", "silver", "gold", "ops"):
        ch(f"CREATE DATABASE IF NOT EXISTS {db}")
    ch("""CREATE TABLE IF NOT EXISTS ops.pii_pads (ipad_hex String, opad_hex String)
          ENGINE = MergeTree ORDER BY tuple()""")
    ipad, opad = pads(key)
    current = ch("SELECT ipad_hex, opad_hex FROM ops.pii_pads FORMAT TSV").strip()
    if current != f"{ipad}\t{opad}":
        ch("TRUNCATE TABLE ops.pii_pads")
        ch("INSERT INTO ops.pii_pads (ipad_hex, opad_hex) FORMAT TSV", f"{ipad}\t{opad}\n")
        log("PII key pads written to ops.pii_pads")
    ch("""CREATE TABLE IF NOT EXISTS ops.dbt_runs
          (started_at DateTime DEFAULT now(), kind LowCardinality(String), duration_s Float32,
           ok UInt8, exit_code Int32, summary String)
          ENGINE = MergeTree ORDER BY started_at TTL started_at + INTERVAL 30 DAY""")


def dbt(command):
    cmd = ["dbt", command, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR,
           "--target-path", f"{STATE_DIR}/target", "--log-path", f"{STATE_DIR}/logs", "--no-use-colors"]
    t0 = time.time()
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=RUN_TIMEOUT_S)
        code, out = res.returncode, res.stdout + res.stderr
    except subprocess.TimeoutExpired:
        code, out = 124, "timeout"
    duration = time.time() - t0
    lines = [l for l in out.splitlines() if l.strip()]
    summary = next((l.split(" ", 2)[-1] for l in reversed(lines) if "Done. PASS" in l), (lines[-1] if lines else ""))
    ok = int(code == 0)
    ch("INSERT INTO ops.dbt_runs (kind, duration_s, ok, exit_code, summary) FORMAT JSONEachRow",
       json.dumps({"kind": command, "duration_s": round(duration, 1), "ok": ok, "exit_code": code,
                   "summary": summary[:500]}))
    log(f"dbt {command}: {'OK' if ok else 'FAILED'} in {duration:.0f}s | {summary[:160]}")
    if not ok:
        log("\n".join(lines[-15:]))
    return duration


def main():
    signal.signal(signal.SIGTERM, lambda *_: globals().update(stop=True))
    signal.signal(signal.SIGINT, lambda *_: globals().update(stop=True))
    key = load_or_create_key()
    for _ in range(60):
        try:
            setup_ops(key)
            break
        except Exception as exc:
            log(f"waiting for ClickHouse: {exc}")
            time.sleep(3)
    else:
        sys.exit("ClickHouse not reachable")
    os.makedirs(STATE_DIR, exist_ok=True)
    last_test = 0.0
    while not stop:
        started = time.time()
        try:
            dbt("run")
            if time.time() - last_test >= TEST_EVERY_S:
                dbt("test")
                last_test = time.time()
        except Exception as exc:       # never let one bad cycle kill the loop
            log(f"cycle failed: {exc}")
        # back to back while a run takes longer than the interval (catching up), otherwise wait
        wait = max(0.0, INTERVAL_S - (time.time() - started))
        end = time.time() + wait
        while not stop and time.time() < end:
            time.sleep(0.5)
    log("stopped")


if __name__ == "__main__":
    main()
