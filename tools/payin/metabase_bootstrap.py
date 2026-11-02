"""Idempotent Metabase setup: admin user, settings, data sources, baseline + optimized cards and dashboards."""
import logging
import os
import sys
import urllib.parse
import urllib.request

import psycopg

from . import cards as C
from .common import setup_logging
from .metabase_api import Metabase, MetabaseError

log = logging.getLogger("metabase_bootstrap")

COLLECTION = "Payin Data Platform"
PG_NAME = "Payin core (operational Postgres)"
CH_NAME = "Payin analytics (ClickHouse)"

CH_URL = os.getenv("CH_URL", "http://clickhouse:8123")
CH_ADMIN_USER = os.getenv("CH_USER", "admin")
CH_ADMIN_PASSWORD = os.getenv("CH_PASSWORD", "ch_pw")
BI_USER, BI_PASSWORD = "bi_ro", "bi_ro_pw"          # same name on both databases; demo credentials


def ensure_bi_role():
    """Read-only Postgres user for the BASELINE dashboard: transactions + the non-PII merchant columns only."""
    with psycopg.connect(autocommit=True) as conn:
        conn.execute("""DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_ro') THEN
                CREATE ROLE bi_ro LOGIN PASSWORD 'bi_ro_pw';
            END IF; END $$""")
        conn.execute("ALTER ROLE bi_ro SET default_transaction_read_only = on")
        conn.execute("GRANT USAGE ON SCHEMA core TO bi_ro")
        conn.execute("GRANT SELECT ON core.transactions TO bi_ro")
        conn.execute("REVOKE SELECT ON core.merchants FROM bi_ro")      # also drops older table-level/column grants
        conn.execute("GRANT SELECT (merchant_id, name, mcc, province, risk_level, updated_at) "
                     "ON core.merchants TO bi_ro")
        conn.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA core REVOKE SELECT ON TABLES FROM bi_ro")


def ch_exec(sql):
    q = urllib.parse.urlencode({"user": CH_ADMIN_USER, "password": CH_ADMIN_PASSWORD})
    req = urllib.request.Request(f"{CH_URL}/?{q}", data=sql.encode(), method="POST")
    urllib.request.urlopen(req, timeout=60).read()


def ensure_ch_reader():
    """Dashboard user on ClickHouse: SELECT on the gold database only. A database-level grant survives the
    table rebuilds of dbt (dim_merchant is dropped and recreated on every run)."""
    ch_exec("CREATE DATABASE IF NOT EXISTS gold")
    ch_exec(f"CREATE USER IF NOT EXISTS {BI_USER} IDENTIFIED BY '{BI_PASSWORD}'")
    ch_exec(f"ALTER USER {BI_USER} IDENTIFIED BY '{BI_PASSWORD}'")
    ch_exec(f"GRANT SELECT ON gold.* TO {BI_USER}")


def set_settings(mb):
    for key, value in [("report-timezone", C.TZ), ("enable-query-caching", False),
                       ("anon-tracking-enabled", False), ("check-for-updates", False)]:
        try:
            mb.call("PUT", f"/api/setting/{key}", {"value": value})
        except MetabaseError as exc:
            log.warning("could not set %s: %s", key, exc)


def ensure_database(mb, name, engine, details):
    for db in mb.databases():
        if db["name"] == name:
            mb.call("PUT", f"/api/database/{db['id']}", {"details": details})   # keep credentials current
            return db["id"]
    created = mb.call("POST", "/api/database", {
        "name": name, "engine": engine, "details": details,
        "is_full_sync": True, "is_on_demand": False, "auto_run_queries": True})
    log.info("created data source %r (id %s)", name, created["id"])
    return created["id"]


def ensure_collection(mb):
    for col in mb.collections():
        if col.get("name") == COLLECTION:
            return col["id"]
    return mb.call("POST", "/api/collection", {"name": COLLECTION, "color": "#509EE3"})["id"]


def upsert_card(mb, collection_id, db_id, name, sql, display, viz):
    body = {"name": name, "display": display, "visualization_settings": viz, "collection_id": collection_id,
            "dataset_query": {"type": "native", "database": db_id, "native": {"query": sql}}}
    for card in mb.cards():
        if card["name"] == name:
            mb.call("PUT", f"/api/card/{card['id']}", body)
            return card["id"]
    return mb.call("POST", "/api/card", body)["id"]


def upsert_dashboard(mb, collection_id, name, card_ids):
    dash_id = next((d["id"] for d in mb.dashboards() if d["name"] == name), None)
    if dash_id is None:
        dash_id = mb.call("POST", "/api/dashboard", {"name": name, "collection_id": collection_id})["id"]
    dashcards = []
    for i, cid in enumerate(card_ids):
        dashcards.append({"id": -(i + 1), "card_id": cid, "row": (i // 2) * 7, "col": (i % 2) * 12,
                          "size_x": 12, "size_y": 7, "series": [], "parameter_mappings": [],
                          "visualization_settings": {}})
    mb.call("PUT", f"/api/dashboard/{dash_id}", {"dashcards": dashcards})
    return dash_id


def check_clickhouse(mb, db_id):
    res = mb.call("POST", "/api/dataset", {"database": db_id, "type": "native", "native": {"query": "SELECT 1 AS ok"}})
    ok = res.get("status") == "completed" and res["data"]["rows"] == [[1]]
    log.info("ClickHouse driver check: %s", "OK (SELECT 1 returned 1)" if ok else f"FAILED: {res.get('error')}")


def main():
    setup_logging()
    ensure_bi_role()
    ensure_ch_reader()
    mb = Metabase()
    mb.wait_until_ready()
    mb.login()
    set_settings(mb)

    pg_id = ensure_database(mb, PG_NAME, "postgres", {
        "host": "postgres", "port": 5432, "dbname": "payin", "user": BI_USER, "password": BI_PASSWORD,
        "ssl": False, "tunnel-enabled": False})

    ch_id = None
    try:
        ch_id = ensure_database(mb, CH_NAME, "clickhouse", {
            "host": "clickhouse", "port": 8123, "dbname": "gold", "user": BI_USER, "password": BI_PASSWORD,
            "ssl": False, "scan-all-databases": True})
        check_clickhouse(mb, ch_id)
    except MetabaseError as exc:
        log.warning("ClickHouse data source not created (driver jar missing in metabase/plugins? run "
                    "'make fetch-driver' then 'make restart-metabase'): %s", exc)

    col = ensure_collection(mb)
    ids = [upsert_card(mb, col, pg_id, C.BASELINE_PREFIX + c["title"], c["core_sql"], c["display"], c["viz"])
           for c in C.CARDS]
    dash = upsert_dashboard(mb, col, C.BASELINE_DASHBOARD, ids)
    log.info("baseline dashboard ready: http://localhost:3000/dashboard/%s  (login %s)", dash,
             os.getenv("MB_ADMIN_EMAIL", "admin@payin.local"))

    if ch_id is not None:
        gids = [upsert_card(mb, col, ch_id, C.OPTIMIZED_PREFIX + c["title"], c["gold_sql"], c["display"], c["viz"])
                for c in C.CARDS]
        gdash = upsert_dashboard(mb, col, C.OPTIMIZED_DASHBOARD, gids)
        log.info("optimized dashboard ready: http://localhost:3000/dashboard/%s", gdash)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:     # fail loudly so `docker compose logs metabase-bootstrap` shows the cause
        log.error("bootstrap failed: %s", exc)
        sys.exit(1)