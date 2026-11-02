"""Access control: the dashboard users are read-only and see only what the dashboards need.   make test-access

ClickHouse bi_ro: SELECT on gold.* only (no bronze/silver/ops, no writes, no grants).
PostgreSQL  bi_ro: SELECT on core.transactions and the non-PII merchant columns, no writes.
"""
import urllib.error
import urllib.parse
import urllib.request

import psycopg
import pytest

from payin import cards as C

from .helpers import CH_URL, ch_rows, ch_value

BI_USER, BI_PASSWORD = "bi_ro", "bi_ro_pw"
GOLD_OBJECTS = ["agg_daily_channel", "agg_merchant_month", "agg_fee_day_mcc_province", "agg_month_risk",
                "fct_transactions", "dim_merchant"]


def ch_as_bi(sql):
    params = urllib.parse.urlencode({"user": BI_USER, "password": BI_PASSWORD})
    req = urllib.request.Request(f"{CH_URL}/?{params}", data=sql.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(errors="replace")


def assert_denied(sql):
    status, body = ch_as_bi(sql)
    assert status != 200 and ("ACCESS_DENIED" in body or "Not enough privileges" in body), (sql, status, body[:300])


@pytest.fixture(scope="module", autouse=True)
def bootstrap_done():
    assert ch_value("SELECT count() FROM system.users WHERE name = 'bi_ro'") == 1, \
        "ClickHouse user bi_ro missing: run `make bootstrap-metabase` first"


@pytest.fixture(scope="module")
def bi_pg():
    with psycopg.connect(user=BI_USER, password=BI_PASSWORD, autocommit=True) as conn:   # host/db from PG* env
        yield conn


def test_dashboard_user_can_read_every_gold_object():
    for obj in GOLD_OBJECTS:
        status, body = ch_as_bi(f"SELECT count() FROM gold.{obj}")
        assert status == 200 and int(body.strip()) > 0, (obj, status, body[:200])


def test_every_optimized_card_runs_as_the_dashboard_user():
    for c in C.CARDS:
        status, body = ch_as_bi(c["gold_sql"])
        assert status == 200 and body.strip(), (c["key"], status, body[:300])


def test_dashboard_user_cannot_read_bronze_silver_or_ops():
    for table in ("bronze.transactions_cdc", "bronze.merchants_cdc", "silver.merchants_current",
                  "silver.stg_merchants_events", "ops.pii_pads"):
        assert_denied(f"SELECT count() FROM {table}")


def test_dashboard_user_cannot_modify_or_escalate_in_clickhouse():
    try:
        assert_denied("CREATE TABLE gold.__bi_probe (x UInt8) ENGINE = Memory")
        assert_denied("INSERT INTO gold.dim_merchant SELECT * FROM gold.dim_merchant LIMIT 0")
        assert_denied("ALTER TABLE gold.dim_merchant DELETE WHERE 0")
        assert_denied("GRANT SELECT ON silver.* TO bi_ro")
    finally:
        ch_rows("DROP TABLE IF EXISTS gold.__bi_probe")      # only exists if the first assertion failed


def test_postgres_dashboard_user_is_read_only_and_cannot_read_owner_pii(bi_pg):
    assert bi_pg.execute("SELECT count(merchant_id) FROM core.merchants").fetchone()[0] > 0
    assert bi_pg.execute("SELECT count(transaction_id) FROM core.transactions").fetchone()[0] > 0
    denied = (psycopg.errors.InsufficientPrivilege, psycopg.errors.ReadOnlySqlTransaction)
    for sql in ("UPDATE core.transactions SET status = status WHERE false",
                "DELETE FROM core.transactions WHERE false",
                "INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status) "
                "SELECT 1, 'POS', '000000', 1, 0, 'PENDING' WHERE false",
                "UPDATE core.merchants SET name = name WHERE false"):
        with pytest.raises(denied):
            bi_pg.execute(sql)
    for col in ("owner_national_id", "owner_mobile"):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            bi_pg.execute(f"SELECT {col} FROM core.merchants LIMIT 1")