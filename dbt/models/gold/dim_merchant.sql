{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = '(merchant_id, valid_from)',
    schema = 'gold'
) }}
{#
  SCD Type 2 merchant dimension, built from the CHANGE EVENTS (not from snapshots), so every real change is kept.
  Grain: one row per merchant per VERSION (a period during which risk_level / province / mcc / name were constant).
  * a new version starts only when a tracked attribute changes (an update that only moves updated_at creates none)
  * valid_from = the event's business time (merchants.updated_at); the FIRST version starts at 1970-01-01, i.e. the
    earliest known state is assumed to hold for all earlier times
  * valid_to = valid_from of the next version; the current version ends at 2299-12-31 (max of DateTime64)
  * versions are ordered by (source_lsn, kafka_offset) = commit order
  Rebuilt completely on every run: the dimension is tiny (about 2,000 merchants, a few thousand events); the large
  fact table is the part that is processed incrementally.
#}
WITH ev AS (
    SELECT
        merchant_id, source_lsn, kafka_offset,
        name, mcc, province, risk_level,
        ifNull(updated_at, toDateTime64(0, 6, 'UTC')) AS business_ts,
        (name, mcc, province, risk_level) AS attrs
    FROM {{ ref('merchants_events') }}
    WHERE is_deleted = 0
),
marked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY merchant_id ORDER BY source_lsn, kafka_offset) AS rn,
        lagInFrame(attrs, 1, ('', '', '', '')) OVER (PARTITION BY merchant_id ORDER BY source_lsn, kafka_offset
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev_attrs
    FROM ev
),
versions AS (
    SELECT
        merchant_id, source_lsn, kafka_offset,
        name, mcc, province, risk_level,
        if(rn = 1, toDateTime64('1970-01-01 00:00:00', 6, 'UTC'), business_ts) AS valid_from
    FROM marked
    WHERE rn = 1 OR attrs != prev_attrs
)
SELECT *, toUInt8(valid_to = toDateTime64('2299-12-31 00:00:00', 6, 'UTC')) AS is_current
FROM (
SELECT
    merchant_id,
    row_number() OVER (PARTITION BY merchant_id ORDER BY source_lsn, kafka_offset) AS version_no,
    valid_from,
    leadInFrame(valid_from, 1, toDateTime64('2299-12-31 00:00:00', 6, 'UTC')) OVER (
        PARTITION BY merchant_id ORDER BY source_lsn, kafka_offset
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS valid_to,
    name, mcc, province, risk_level,
    kafka_offset AS first_kafka_offset
FROM versions
)
