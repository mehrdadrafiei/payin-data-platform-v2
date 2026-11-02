#!/usr/bin/env python3
"""Check 5: ClickHouse has no native HMAC (we think) -> build HMAC-SHA256 from SHA256 + precomputed key pads
and compare with Python's hmac on sample values."""
import hashlib
import hmac
import subprocess
import sys

KEY = b"dev-only-secret-key"  # must be <= 64 bytes (longer keys are hashed first, handled below)
SAMPLES = ["0012345678", "09121234567", ""]


def pads(key: bytes):
    if len(key) > 64:
        key = hashlib.sha256(key).digest()
    k = key.ljust(64, b"\x00")
    return bytes(b ^ 0x36 for b in k).hex(), bytes(b ^ 0x5C for b in k).hex()


def ch(sql: str) -> str:
    r = subprocess.run(
        ["docker", "compose", "exec", "-T", "clickhouse", "clickhouse-client",
         "--user", "admin", "--password", "ch_pw", "-q", sql],
        capture_output=True, text=True)
    if r.returncode:
        print(r.stderr)
        sys.exit(1)
    return r.stdout.strip()


ipad, opad = pads(KEY)
print("native HMAC-like functions in ClickHouse:",
      ch("SELECT groupArray(name) FROM system.functions WHERE name ILIKE '%hmac%'"))
ok = True
for s in SAMPLES:
    expected = hmac.new(KEY, s.encode(), hashlib.sha256).hexdigest()
    got = ch(f"SELECT lower(hex(SHA256(concat(unhex('{opad}'), SHA256(concat(unhex('{ipad}'), '{s}'))))))")
    status = "OK" if got == expected else "MISMATCH"
    ok &= got == expected
    print(f"  sample {s!r:14} {status}")
sys.exit(0 if ok else 1)
