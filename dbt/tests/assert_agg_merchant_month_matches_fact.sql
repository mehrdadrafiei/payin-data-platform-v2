(
    SELECT txn_month, merchant_id, countIf(status = 'SUCCESS') AS success_count, sumIf(amount, status = 'SUCCESS') AS success_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_month, merchant_id HAVING success_count > 0
    EXCEPT
    SELECT txn_month, merchant_id, success_count, success_amount FROM {{ ref('agg_merchant_month') }}
)
UNION ALL
(
    SELECT txn_month, merchant_id, success_count, success_amount FROM {{ ref('agg_merchant_month') }}
    EXCEPT
    SELECT txn_month, merchant_id, countIf(status = 'SUCCESS') AS success_count, sumIf(amount, status = 'SUCCESS') AS success_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_month, merchant_id HAVING success_count > 0
)
