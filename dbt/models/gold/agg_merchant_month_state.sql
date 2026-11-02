{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(_version)',
    order_by = '(txn_month, merchant_id)',
    
    schema = 'gold'
) }}
{# Grain: Tehran month x merchant. Successful count and amount. Serves card 3 (top 10 merchants of the month). #}
{{ incremental_aggregate(ref('fct_transactions_state'), 'txn_month', ['txn_month', 'merchant_id'], [
    ['success_count', "countIf(status = 'SUCCESS')"],
    ['success_amount', "sumIf(amount, status = 'SUCCESS')"]
]) }}
