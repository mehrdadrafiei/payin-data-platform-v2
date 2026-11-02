{{ config(materialized = 'view', schema = 'silver') }}
{# every distinct change event exactly once (FINAL removes duplicates) #}
SELECT * FROM {{ ref('stg_transactions_events') }} FINAL
