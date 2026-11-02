{{ config(materialized = 'view', schema = 'gold') }}
{# Latest version of every group; groups without any (successful) activity are hidden. #}
SELECT txn_date, channel, total_count, pending_count, success_count, failed_count, reversed_count, success_amount
FROM {{ ref('agg_daily_channel_state') }} FINAL
WHERE total_count > 0
