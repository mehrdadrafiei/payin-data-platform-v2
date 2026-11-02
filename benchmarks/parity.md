# Parity and reconciliation report

Run at 2026-11-02T19:28:38+00:00 UTC, 2 attempt(s). Exact checks must pass; INFO lines describe intended differences.

| Check | Result |
|---|---|
| R1 transactions per status: core == silver == gold | PASS |
| R2 successful count/amount per Tehran day (all history): core == gold | PASS |
| 1. Daily successful amount and count (last 90 days): baseline == optimized | PASS |
| 2. Daily success rate by channel (last 30 days): baseline == optimized | PASS |
| 3. Top 10 merchants this month by successful amount: baseline == optimized | PASS |
| card 4: baseline re-derived from gold fact (current province) == baseline | PASS |
| card 4: fee totals per MCC, optimized == baseline | PASS |
| card 5: baseline re-derived from gold fact (current risk) == baseline | PASS |
| card 5: amount and count per month, optimized == baseline | PASS |

INFO: 3.2% of fee amount sits in a different province than the merchant's current one (province at transaction time)

INFO: 12.4% of successful amount sits in a different risk level than the merchant's current one (risk level at transaction time)
