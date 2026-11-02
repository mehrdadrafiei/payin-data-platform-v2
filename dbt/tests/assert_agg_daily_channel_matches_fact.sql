{# The aggregate must equal a fresh aggregation of the fact. Any row returned is a difference (in either direction). #}
(
    SELECT txn_date, channel, count() AS total_count,
           countIf(status = 'PENDING') AS pending_count, countIf(status = 'SUCCESS') AS success_count,
           countIf(status = 'FAILED') AS failed_count, countIf(status = 'REVERSED') AS reversed_count,
           sumIf(amount, status = 'SUCCESS') AS success_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_date, channel
    EXCEPT
    SELECT txn_date, channel, total_count, pending_count, success_count, failed_count, reversed_count, success_amount
    FROM {{ ref('agg_daily_channel') }}
)
UNION ALL
(
    SELECT txn_date, channel, total_count, pending_count, success_count, failed_count, reversed_count, success_amount
    FROM {{ ref('agg_daily_channel') }}
    EXCEPT
    SELECT txn_date, channel, count() AS total_count,
           countIf(status = 'PENDING') AS pending_count, countIf(status = 'SUCCESS') AS success_count,
           countIf(status = 'FAILED') AS failed_count, countIf(status = 'REVERSED') AS reversed_count,
           sumIf(amount, status = 'SUCCESS') AS success_amount
    FROM {{ ref('fct_transactions') }} GROUP BY txn_date, channel
)
