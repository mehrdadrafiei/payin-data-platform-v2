import psycopg
import pytest


@pytest.fixture(scope="session")
def pg():
    """Superuser connection to the operational database (uses the PG* environment variables)."""
    with psycopg.connect(autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="session")
def pipeline_ready():
    """Wait (up to 30 minutes, first start only) until silver and gold have caught up with bronze."""
    from .helpers import caught_up, eventually, pipeline_lag
    eventually(caught_up, timeout=1800, interval=10, what=f"silver/gold to catch up with bronze (lag: {pipeline_lag()})")
