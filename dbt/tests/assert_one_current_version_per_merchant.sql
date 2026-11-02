{# Exactly one open (current) version per merchant. #}
SELECT merchant_id, sum(is_current) AS current_versions
FROM {{ ref('dim_merchant') }}
GROUP BY merchant_id
HAVING current_versions != 1
