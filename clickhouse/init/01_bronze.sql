-- Medallion layers
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- BRONZE: immutable CDC event log. ClickHouse itself consumes the Kafka topics (Kafka engine),
-- so ingestion does not depend on dbt or any other service. Never updated, never deleted, no TTL.
-- 'stream' error mode: unparseable messages land in bronze.cdc_errors instead of blocking ingestion.

CREATE TABLE bronze.cdc_errors
(
    _topic        LowCardinality(String),
    _partition    UInt64,
    _offset       UInt64,
    _ingested_at  DateTime64(3) DEFAULT now64(3),
    raw_message   String,
    error         String
)
ENGINE = MergeTree
ORDER BY (_topic, _offset);

-- ---------------------------------------------------------------------------
-- merchants
-- ---------------------------------------------------------------------------
CREATE TABLE bronze.merchants_kafka
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'payin.core.merchants',
    kafka_group_name = 'ch_bronze_merchants',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE bronze.merchants_cdc
(
    _topic        LowCardinality(String),
    _partition    UInt64,
    _offset       UInt64,
    _kafka_ts     DateTime,
    _ingested_at  DateTime64(3) DEFAULT now64(3),
    op            LowCardinality(String),   -- r=snapshot read, c=insert, u=update, d=delete
    source_lsn    UInt64,                   -- position in the Postgres WAL
    source_ts_ms  Int64,                    -- commit time at the source (ms)
    pk            Int64,                    -- merchant_id
    payload       String CODEC(ZSTD(3))     -- full Debezium envelope (before/after/source/op)
)
ENGINE = MergeTree
ORDER BY _offset;

CREATE MATERIALIZED VIEW bronze.merchants_cdc_mv TO bronze.merchants_cdc AS
SELECT
    _topic,
    _partition,
    _offset,
    ifNull(_timestamp, toDateTime(0)) AS _kafka_ts,
    JSONExtractString(raw, 'op') AS op,
    JSONExtractUInt(raw, 'source', 'lsn') AS source_lsn,
    JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms,
    if(op = 'd',
       JSONExtractInt(raw, 'before', 'merchant_id'),
       JSONExtractInt(raw, 'after', 'merchant_id')) AS pk,
    raw AS payload
FROM bronze.merchants_kafka
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW bronze.merchants_errors_mv TO bronze.cdc_errors AS
SELECT
    _topic,
    _partition,
    _offset,
    _raw_message AS raw_message,
    _error AS error
FROM bronze.merchants_kafka
WHERE length(_error) > 0;

-- ---------------------------------------------------------------------------
-- transactions
-- ---------------------------------------------------------------------------
CREATE TABLE bronze.transactions_kafka
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'payin.core.transactions',
    kafka_group_name = 'ch_bronze_transactions',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE bronze.transactions_cdc
(
    _topic        LowCardinality(String),
    _partition    UInt64,
    _offset       UInt64,
    _kafka_ts     DateTime,
    _ingested_at  DateTime64(3) DEFAULT now64(3),
    op            LowCardinality(String),   -- r=snapshot read, c=insert, u=update, d=delete
    source_lsn    UInt64,                   -- position in the Postgres WAL
    source_ts_ms  Int64,                    -- commit time at the source (ms)
    pk            Int64,                    -- transaction_id
    payload       String CODEC(ZSTD(3))     -- full Debezium envelope (before/after/source/op)
)
ENGINE = MergeTree
ORDER BY _offset;

CREATE MATERIALIZED VIEW bronze.transactions_cdc_mv TO bronze.transactions_cdc AS
SELECT
    _topic,
    _partition,
    _offset,
    ifNull(_timestamp, toDateTime(0)) AS _kafka_ts,
    JSONExtractString(raw, 'op') AS op,
    JSONExtractUInt(raw, 'source', 'lsn') AS source_lsn,
    JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms,
    if(op = 'd',
       JSONExtractInt(raw, 'before', 'transaction_id'),
       JSONExtractInt(raw, 'after', 'transaction_id')) AS pk,
    raw AS payload
FROM bronze.transactions_kafka
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW bronze.transactions_errors_mv TO bronze.cdc_errors AS
SELECT
    _topic,
    _partition,
    _offset,
    _raw_message AS raw_message,
    _error AS error
FROM bronze.transactions_kafka
WHERE length(_error) > 0;
