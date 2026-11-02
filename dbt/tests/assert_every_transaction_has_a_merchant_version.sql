{# A transaction without a matching merchant version would silently get empty province / risk level. #}
SELECT transaction_id, merchant_id, created_at
FROM {{ ref('fct_transactions') }}
WHERE merchant_version_found = 0
