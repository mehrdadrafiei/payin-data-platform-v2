"""Idempotent Metabase setup: admin user, settings, data sources, baseline cards and dashboard."""
import logging
import os
import sys

import psycopg

from . import cards as C
from .common import setup_logging
from .metabase_api import Metabase, MetabaseError

log = logging.getLogger("metabase_bootstrap")

COLLECTION = "Payin Data Platform"
PG_NAME = "Payin core (operational Postgres)"
CH_NAME = "Payin analytics (ClickHouse)"


def ensure_bi_role():
    """Read-only Postgres user for Metabase (baseline queries must not be able to change the source)."""
    with psycopg.connect(autocommit=True) as conn:
        conn.execute("""DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_ro') THEN
                CREATE ROLE bi_ro LOGIN PASSWORD 'bi_ro_pw';
            END IF; END $$""")
        conn.execute("ALTER ROLE bi_ro SET default_transaction_read_only = on")
        conn.execute("GRANT USAGE ON SCHEMA core TO bi_ro")
        conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA core TO bi_ro")
        conn.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO bi_ro")


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
    mb = Metabase()
    mb.wait_until_ready()
    mb.login()
    set_settings(mb)

    pg_id = ensure_database(mb, PG_NAME, "postgres", {
        "host": "postgres", "port": 5432, "dbname": "payin", "user": "bi_ro", "password": "bi_ro_pw",
        "ssl": False, "tunnel-enabled": False})

    try:
        ch_id = ensure_database(mb, CH_NAME, "clickhouse", {
            "host": "clickhouse", "port": 8123, "dbname": "default", "user": "admin", "password": "ch_pw",
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


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:     # fail loudly so `docker compose logs metabase-bootstrap` shows the cause
        log.error("bootstrap failed: %s", exc)
        sys.exit(1)
