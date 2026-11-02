(
    SELECT txn_month, risk_level_at_txn, countIf(status = 'SUCCESS') AS success_count, sumIf(amount, status = 'SUCCESS') AS success_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_month, risk_level_at_txn HAVING success_count > 0
    EXCEPT
    SELECT txn_month, risk_level_at_txn, success_count, success_amount FROM {{ ref('agg_month_risk') }}
)
UNION ALL
(
    SELECT txn_month, risk_level_at_txn, success_count, success_amount FROM {{ ref('agg_month_risk') }}
    EXCEPT
    SELECT txn_month, risk_level_at_txn, countIf(status = 'SUCCESS') AS success_count, sumIf(amount, status = 'SUCCESS') AS success_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_month, risk_level_at_txn HAVING success_count > 0
)
