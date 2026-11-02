"""Shared helpers for the seed-history job and the change simulator."""
import logging
import time

import psycopg

PROVINCES = [
    "Tehran", "Isfahan", "Fars", "Khorasan Razavi", "East Azerbaijan", "Khuzestan", "Mazandaran",
    "Alborz", "Gilan", "Kerman", "Qom", "Yazd", "Hormozgan", "Kermanshah", "West Azerbaijan",
]

# Simple risk model: where a merchant's risk level can move next, with probabilities.
RISK_NEXT = {
    "LOW": (("MEDIUM", 0.85), ("HIGH", 0.15)),
    "MEDIUM": (("LOW", 0.50), ("HIGH", 0.50)),
    "HIGH": (("MEDIUM", 0.70), ("LOW", 0.30)),
}


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def next_risk(current: str, rng) -> str:
    r = rng.random()
    acc = 0.0
    for state, p in RISK_NEXT[current]:
        acc += p
        if r < acc:
            return state
    return RISK_NEXT[current][-1][0]


def next_province(current: str, rng) -> str:
    return rng.choice([p for p in PROVINCES if p != current])


def connect(retries: int = 60, delay: float = 2.0) -> psycopg.Connection:
    """Connect using the standard PG* environment variables; retry while Postgres is starting."""
    last = None
    for _ in range(retries):
        try:
            return psycopg.connect(autocommit=True)
        except psycopg.OperationalError as exc:
            last = exc
            time.sleep(delay)
    raise last
