# Synthetic data: what is generated and how

Everything in this project is synthetic. Nothing comes from a real payment company or real people.

## Base data (`seed/seed_base.sql`, runs once)
* 2,000 merchants, 5,000,000 transactions spread evenly over the last 180 days. A fixed random seed makes the
  data reproducible.
* Status mix of the seeded transactions: 80% SUCCESS, 10% FAILED, 10% REVERSED. Channel mix about 55% POS,
  30% IPG, 15% QR. Merchant popularity is skewed (a few large merchants), so "top 10" is meaningful.
* Owner national ids and mobile numbers are fake numbers. About 25% of merchants share an owner.
* Every merchant starts in one state dated 181 days ago (before the first transaction).

## Merchant history (`tools/payin/seed_history.py`, runs once)
Change data capture only sees changes made after it starts, and `core.merchants` holds one row per merchant.
To get a realistic 180-day history of `risk_level` and `province`, a job applies about 3,000 changes to
`core.merchants` once the replication slot exists:

* each merchant gets 0-3 changes (1.5 on average); 85% change the risk level, 15% change the province;
* risk moves along a simple chain (LOW to MEDIUM is the most likely first step);
* every change is written with a **backdated `updated_at`** (between 175 days ago and 12 hours ago),
  applied in chronological order.

Consequence: for these changes the **commit time is "now"** but the **business time (`updated_at`) is in the
past**. Real change streams never look like that. Therefore:
* point-in-time logic uses `updated_at` (business time) to decide which merchant version applied to a
  transaction;
* any lag or freshness metric must be measured from the commit time, never from `updated_at`.

## Live changes (`tools/payin/simulator.py`, runs continuously)
About 20 new PENDING transactions per second, resolved to SUCCESS/FAILED after 2-10 seconds; about 0.8
reversals per second (90% recent transactions, 10% up to 7 days old, so old days must be corrected);
short-lived test transactions (card bin `000000`) that are hard-deleted after 20-60 seconds; one merchant
changes risk level and province every minute (configurable). When the column `installments` is added, the
simulator starts filling it. After a simulator restart, still-pending rows are rescheduled for the first simulator
tick at or after 10 seconds from creation; the outcome is sampled again because the prior in-memory schedule is lost.
Unit tests in `tools/tests/test_logic.py` cover the generators and the restart-recovery timing.