{{ config(materialized = 'view', schema = 'gold') }}
{# Latest version of every group; groups without any (successful) activity are hidden. #}
SELECT txn_date, mcc, province_at_txn, success_count, success_fee_amount
FROM {{ ref('agg_fee_day_mcc_province_state') }} FINAL
WHERE success_count > 0
