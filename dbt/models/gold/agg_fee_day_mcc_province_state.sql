{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(_version)',
    order_by = '(txn_date, mcc, province_at_txn)',
    partition_by = 'toYYYYMM(txn_date)',
    schema = 'gold'
) }}
{# Grain: Tehran day x MCC x province AT TRANSACTION TIME. Fees of successful transactions. Serves card 4. #}
{{ incremental_aggregate(ref('fct_transactions_state'), 'txn_date', ['txn_date', 'mcc', 'province_at_txn'], [
    ['success_count', "countIf(status = 'SUCCESS')"],
    ['success_fee_amount', "sumIf(fee_amount, status = 'SUCCESS')"]
]) }}
