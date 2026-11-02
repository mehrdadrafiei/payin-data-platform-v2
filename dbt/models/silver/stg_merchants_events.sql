{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(kafka_offset)',
    order_by = '(merchant_id, source_lsn, op)',
    schema = 'silver'
) }}
{# One row per change event, typed. Personal data is replaced by HMAC tokens here and never stored in clear text.
   Duplicates (same merchant, LSN, op) can appear because Debezium is at-least-once and runs overlap;
   the engine keeps one row per identity after merges, and the merchants_events view reads with FINAL. #}
WITH src AS (
    SELECT _offset AS kafka_offset, op, source_lsn, source_ts_ms, _ingested_at AS ingested_at, pk, payload
    FROM {{ source('bronze', 'merchants_cdc') }}
    WHERE {{ offset_window('_offset', source('bronze', 'merchants_cdc')) }}
),
parsed AS (
    SELECT
        pk AS merchant_id,
        op,
        if(op = 'd', toUInt8(1), toUInt8(0)) AS is_deleted,
        source_lsn,
        kafka_offset,
        fromUnixTimestamp64Milli(source_ts_ms, 'UTC') AS source_ts,
        ingested_at,
        JSONExtractString(payload, 'after', 'name') AS name,
        JSONExtractString(payload, 'after', 'mcc') AS mcc,
        JSONExtractString(payload, 'after', 'province') AS province,
        JSONExtractString(payload, 'after', 'risk_level') AS risk_level,
        parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'after', 'updated_at'), 6, 'UTC') AS updated_at,
        JSONExtractString(payload, 'after', 'owner_national_id') AS nid_raw,
        JSONExtractString(payload, 'after', 'owner_mobile') AS mobile_raw
    FROM src
)
SELECT
    concat(toString(merchant_id), '-', toString(source_lsn), '-', op) AS event_key,
    merchant_id, op, is_deleted, source_lsn, kafka_offset, source_ts, ingested_at,
    name, mcc, province, risk_level, updated_at,
    {{ hmac_sha256('nid_raw') }} AS owner_national_id_token,
    {{ hmac_sha256('mobile_raw') }} AS owner_mobile_token,
    {{ mask_mobile('mobile_raw') }} AS owner_mobile_masked
FROM parsed
