(
    SELECT txn_date, mcc, province_at_txn, countIf(status = 'SUCCESS') AS success_count,
           sumIf(fee_amount, status = 'SUCCESS') AS success_fee_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_date, mcc, province_at_txn HAVING success_count > 0
    EXCEPT
    SELECT txn_date, mcc, province_at_txn, success_count, success_fee_amount FROM {{ ref('agg_fee_day_mcc_province') }}
)
UNION ALL
(
    SELECT txn_date, mcc, province_at_txn, success_count, success_fee_amount FROM {{ ref('agg_fee_day_mcc_province') }}
    EXCEPT
    SELECT txn_date, mcc, province_at_txn, countIf(status = 'SUCCESS') AS success_count,
           sumIf(fee_amount, status = 'SUCCESS') AS success_fee_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_date, mcc, province_at_txn HAVING success_count > 0
)
