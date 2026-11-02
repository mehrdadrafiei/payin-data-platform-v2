{{ config(materialized = 'view', schema = 'gold') }}
{# Grain: one row per current (not deleted) transaction, with the merchant's province / risk level / mcc
   valid at transaction time. FINAL = latest version of every transaction. Days are Asia/Tehran days. #}
SELECT
    transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status,
    created_at, updated_at, txn_date, txn_month, installments,
    mcc, province_at_txn, risk_level_at_txn, merchant_valid_from, merchant_version_found,
    src_kafka_offset
FROM {{ ref('fct_transactions_state') }} FINAL
WHERE is_deleted = 0
