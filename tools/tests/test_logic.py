import random
from datetime import datetime, timedelta, timezone

from payin.common import PROVINCES, RISK_NEXT, next_province, next_risk
from payin.seed_history import plan_history
from payin.simulator import draw_count, new_txn_rows


def test_next_risk_always_changes_and_is_valid():
    rng = random.Random(1)
    for state in RISK_NEXT:
        for _ in range(500):
            assert next_risk(state, rng) in RISK_NEXT and next_risk(state, rng) != state


def test_next_province_changes():
    rng = random.Random(1)
    for p in PROVINCES:
        assert all(next_province(p, rng) != p for _ in range(50))


def test_plan_history_sorted_in_bounds_deterministic():
    start = datetime(2026, 5, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=170)
    a = plan_history(range(1, 2001), start, end, random.Random(42))
    b = plan_history(range(1, 2001), start, end, random.Random(42))
    assert a == b
    assert a == sorted(a)
    assert all(start <= ts <= end for ts, _ in a)
    assert 2700 < len(a) < 3300          # 1.5 changes per merchant on average


def test_new_txn_rows_ranges_and_skew():
    rng = random.Random(3)
    m, c, b, a, i = new_txn_rows(20000, 2000, rng)
    assert min(m) >= 1 and max(m) <= 2000
    assert set(c) <= {"POS", "IPG", "QR"}
    assert all(x % 1000 == 0 and x > 0 for x in a)
    assert None not in i
    top10_share = sum(1 for x in m if x <= 10) / len(m)
    assert 0.04 < top10_share < 0.12      # a few merchants are big


def test_draw_count_matches_rate():
    rng = random.Random(5)
    total = sum(draw_count(0.8, rng) for _ in range(20000))
    assert 0.77 < total / 20000 < 0.83
    assert all(draw_count(2.0, rng) == 2 for _ in range(10))
