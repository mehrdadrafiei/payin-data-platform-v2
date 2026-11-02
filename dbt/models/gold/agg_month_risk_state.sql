{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(_version)',
    order_by = '(txn_month, risk_level_at_txn)',
    
    schema = 'gold'
) }}
{# Grain: Tehran month x merchant risk level AT TRANSACTION TIME. Successful count and amount. Serves card 5. #}
{{ incremental_aggregate(ref('fct_transactions_state'), 'txn_month', ['txn_month', 'risk_level_at_txn'], [
    ['success_count', "countIf(status = 'SUCCESS')"],
    ['success_amount', "sumIf(amount, status = 'SUCCESS')"]
]) }}
