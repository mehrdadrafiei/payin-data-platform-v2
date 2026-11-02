{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    engine = 'ReplacingMergeTree(_version, is_deleted)',
    order_by = '(merchant_id, created_at, transaction_id)',
    partition_by = 'toYYYYMM(txn_date)',
    schema = 'gold'
) }}
{#
  Transaction fact (versioned physical table; read it through the view fct_transactions).
  Grain: one row per transaction (current version). The sort key (merchant_id, created_at, transaction_id) is unique
  per transaction because a transaction's merchant and creation time never change; it lets the engine replace old
  versions and lets the dimension-change step below find one merchant's transactions quickly.
  Merchant attributes are the ones valid AT TRANSACTION TIME (ASOF join of created_at to dim_merchant.valid_from).

  Two sources of work per run:
    (A) transactions whose silver state changed (new, updated, deleted) - read by offset window
    (B) transactions of merchants that got a NEW dimension version: they are recomputed from that version's valid_from
        onwards, because an earlier assignment may now be wrong (a merchant change can arrive after the transactions
        that it applies to, the two topics are independent).
  Deleted transactions are written as is_deleted = 1 rows (with their last known key columns) so that they replace
  the live row and disappear from the view.
#}
WITH changed AS (
    SELECT
        transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status,
        ifNull(created_at, toDateTime64(0, 6, 'UTC')) AS created_at,
        ifNull(updated_at, toDateTime64(0, 6, 'UTC')) AS updated_at,
        installments, is_deleted, kafka_offset AS src_kafka_offset
    FROM {{ ref('transactions_state') }}
    WHERE {{ offset_window('kafka_offset', ref('transactions_state'), 'src_kafka_offset') }}
    ORDER BY transaction_id, source_lsn DESC, kafka_offset DESC
    LIMIT 1 BY transaction_id
),
{% if is_incremental() %}
dim_new AS (
    SELECT merchant_id, min(valid_from) AS from_ts
    FROM {{ ref('dim_merchant') }}
    WHERE first_kafka_offset > (SELECT coalesce(max(dim_offset_seen), 0) FROM {{ this }})
    GROUP BY merchant_id
),
reprocess AS (
    SELECT
        f.transaction_id AS transaction_id, f.merchant_id AS merchant_id, f.channel AS channel,
        f.card_bin AS card_bin, f.amount AS amount, f.fee_amount AS fee_amount, f.status AS status,
        f.created_at AS created_at, f.updated_at AS updated_at, f.installments AS installments,
        toUInt8(0) AS is_deleted, f.src_kafka_offset AS src_kafka_offset
    FROM {{ this }} AS f FINAL
    INNER JOIN dim_new AS d ON f.merchant_id = d.merchant_id
    WHERE f.merchant_id IN (SELECT merchant_id FROM dim_new)
      AND f.created_at >= (SELECT min(from_ts) FROM dim_new)
      AND f.created_at >= d.from_ts
      AND f.is_deleted = 0
),
{% endif %}
base AS (
    SELECT * FROM changed
    {% if is_incremental() -%}
    UNION ALL
    SELECT * FROM reprocess WHERE transaction_id NOT IN (SELECT transaction_id FROM changed)
    {% endif %}
)
SELECT
    b.transaction_id AS transaction_id,
    b.merchant_id AS merchant_id,
    b.channel AS channel,
    b.card_bin AS card_bin,
    b.amount AS amount,
    b.fee_amount AS fee_amount,
    b.status AS status,
    b.created_at AS created_at,
    b.updated_at AS updated_at,
    toDate(toTimeZone(b.created_at, 'Asia/Tehran')) AS txn_date,
    toStartOfMonth(toDate(toTimeZone(b.created_at, 'Asia/Tehran'))) AS txn_month,
    b.installments AS installments,
    d.mcc AS mcc,
    d.province AS province_at_txn,
    d.risk_level AS risk_level_at_txn,
    d.valid_from AS merchant_valid_from,
    toUInt8(d.merchant_id = b.merchant_id) AS merchant_version_found,
    b.is_deleted AS is_deleted,
    b.src_kafka_offset AS src_kafka_offset,
    (SELECT max(first_kafka_offset) FROM {{ ref('dim_merchant') }}) AS dim_offset_seen,
    {{ run_version() }} AS _version
FROM base AS b
ASOF LEFT JOIN {{ ref('dim_merchant') }} AS d
    ON b.merchant_id = d.merchant_id AND b.created_at >= d.valid_from
