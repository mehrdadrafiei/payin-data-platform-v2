{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(kafka_offset)',
    order_by = '(transaction_id, source_lsn, op)',
    schema = 'silver'
) }}
{# One row per change event, typed. Columns added later at the source (installments) are read from the JSON
   payload: events from before the change simply have NULL there. #}
WITH src AS (
    SELECT _offset AS kafka_offset, op, source_lsn, source_ts_ms, _ingested_at AS ingested_at, pk, payload
    FROM {{ source('bronze', 'transactions_cdc') }}
    WHERE {{ offset_window('_offset', source('bronze', 'transactions_cdc')) }}
)
SELECT
    concat(toString(pk), '-', toString(source_lsn), '-', op) AS event_key,
    pk AS transaction_id,
    op,
    if(op = 'd', toUInt8(1), toUInt8(0)) AS is_deleted,
    source_lsn,
    kafka_offset,
    fromUnixTimestamp64Milli(source_ts_ms, 'UTC') AS source_ts,
    ingested_at,
    JSONExtractInt(payload, 'after', 'merchant_id') AS merchant_id,
    JSONExtractString(payload, 'after', 'channel') AS channel,
    JSONExtractString(payload, 'after', 'card_bin') AS card_bin,
    JSONExtractInt(payload, 'after', 'amount') AS amount,
    JSONExtractInt(payload, 'after', 'fee_amount') AS fee_amount,
    JSONExtractString(payload, 'after', 'status') AS status,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'after', 'created_at'), 6, 'UTC') AS created_at,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'after', 'updated_at'), 6, 'UTC') AS updated_at,
    JSONExtract(payload, 'after', 'installments', 'Nullable(Int32)') AS installments
FROM src
