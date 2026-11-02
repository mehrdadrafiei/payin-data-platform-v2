{{ config(materialized = 'view', schema = 'gold') }}
{# Latest version of every group; groups without any (successful) activity are hidden. #}
SELECT txn_month, merchant_id, success_count, success_amount
FROM {{ ref('agg_merchant_month_state') }} FINAL
WHERE success_count > 0
