{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(_version)',
    order_by = '(txn_date, channel)',
    partition_by = 'toYYYYMM(txn_date)',
    schema = 'gold'
) }}
{# Grain: Tehran day x channel. Counts per status and successful amount. Serves cards 1 and 2 (success rate = success / (success + failed + reversed)). #}
{{ incremental_aggregate(ref('fct_transactions_state'), 'txn_date', ['txn_date', 'channel'], [
    ['total_count', 'count()'],
    ['pending_count', "countIf(status = 'PENDING')"],
    ['success_count', "countIf(status = 'SUCCESS')"],
    ['failed_count', "countIf(status = 'FAILED')"],
    ['reversed_count', "countIf(status = 'REVERSED')"],
    ['success_amount', "sumIf(amount, status = 'SUCCESS')"]
]) }}
