{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(source_lsn, is_deleted)',
    order_by = 'transaction_id',
    schema = 'silver'
) }}
{{ state_from_events(
    ref('stg_transactions_events'), 'transaction_id',
    ['merchant_id', 'channel', 'card_bin', 'amount', 'fee_amount', 'status',
     'created_at', 'updated_at', 'installments']) }}
