{{ config(materialized = 'view', schema = 'silver') }}
{# the current merchants: latest version of every key, deleted keys hidden (FINAL applies both rules) #}
SELECT merchant_id, name, mcc, province, risk_level, updated_at,
       owner_national_id_token, owner_mobile_token, owner_mobile_masked,
       source_lsn, kafka_offset
FROM {{ ref('merchants_state') }} FINAL
WHERE is_deleted = 0
