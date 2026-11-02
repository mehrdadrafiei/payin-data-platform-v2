{{ config(materialized = 'view', schema = 'gold') }}
{# Latest version of every group; groups without any (successful) activity are hidden. #}
SELECT txn_month, risk_level_at_txn, success_count, success_amount
FROM {{ ref('agg_month_risk_state') }} FINAL
WHERE success_count > 0
