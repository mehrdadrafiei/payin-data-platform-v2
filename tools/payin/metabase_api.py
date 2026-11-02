"""Minimal Metabase REST client (standard library only)."""
import json
import os
import time
import urllib.error
import urllib.request

BASE = os.getenv("MB_URL", "http://metabase:3000")
ADMIN_EMAIL = os.getenv("MB_ADMIN_EMAIL", "admin@payin.local")
ADMIN_PASSWORD = os.getenv("MB_ADMIN_PASSWORD", "Payin-demo-2026!")


class MetabaseError(RuntimeError):
    pass


class Metabase:
    def __init__(self, base: str = BASE):
        self.base = base.rstrip("/")
        self.session = None

    def call(self, method: str, path: str, body=None, timeout: int = 600):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        if self.session:
            req.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raise MetabaseError(f"{method} {path} -> HTTP {exc.code}: {exc.read().decode(errors='replace')[:600]}")

    def wait_until_ready(self, timeout_s: int = 600):
        start = time.time()
        while True:
            try:
                if (self.call("GET", "/api/health", timeout=10) or {}).get("status") == "ok":
                    return
            except Exception:
                pass
            if time.time() - start > timeout_s:
                raise MetabaseError("Metabase did not become healthy in time")
            time.sleep(3)

    def login(self):
        """Log in; if the instance is brand new (no user yet), create the admin user instead.
        Note: Metabase keeps returning a `setup-token` after setup, so it cannot tell us whether a user exists."""
        try:
            res = self.call("POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        except MetabaseError as login_error:
            token = (self.call("GET", "/api/session/properties") or {}).get("setup-token")
            if not token:
                raise login_error
            res = self.call("POST", "/api/setup", {
                "token": token,
                "user": {"first_name": "Payin", "last_name": "Admin",
                         "email": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
                "prefs": {"site_name": "Payin Data Platform", "site_locale": "en", "allow_tracking": False},
            })
        self.session = res["id"]

    # helpers ---------------------------------------------------------------
    @staticmethod
    def _items(res):
        return res["data"] if isinstance(res, dict) and "data" in res else res

    def databases(self):
        return self._items(self.call("GET", "/api/database"))

    def cards(self):
        return self._items(self.call("GET", "/api/card"))

    def dashboards(self):
        return self._items(self.call("GET", "/api/dashboard"))

    def collections(self):
        return self._items(self.call("GET", "/api/collection"))
