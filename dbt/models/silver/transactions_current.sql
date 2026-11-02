{{ config(materialized = 'view', schema = 'silver') }}
{# the current transactions: latest version of every key, hard-deleted keys hidden #}
SELECT transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status,
       created_at, updated_at, installments, source_lsn, kafka_offset
FROM {{ ref('transactions_state') }} FINAL
WHERE is_deleted = 0
