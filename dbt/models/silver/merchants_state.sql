{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(source_lsn, is_deleted)',
    order_by = 'merchant_id',
    schema = 'silver'
) }}
{{ state_from_events(
    ref('stg_merchants_events'), 'merchant_id',
    ['name', 'mcc', 'province', 'risk_level', 'updated_at',
     'owner_national_id_token', 'owner_mobile_token', 'owner_mobile_masked']) }}
