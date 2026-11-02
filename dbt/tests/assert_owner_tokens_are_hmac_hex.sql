{# Owner identity must only appear as a 64-character lowercase hex HMAC token (or empty), never as digits. #}
SELECT merchant_id, owner_national_id_token
FROM {{ ref('merchants_current') }}
WHERE NOT match(owner_national_id_token, '^[0-9a-f]{64}$')
   OR (owner_mobile_token != '' AND NOT match(owner_mobile_token, '^[0-9a-f]{64}$'))
