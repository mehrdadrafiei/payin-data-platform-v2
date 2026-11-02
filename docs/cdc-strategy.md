# CDC strategy: updates, deletes, ordering, duplicates, schema changes

| Topic | Rule |
|---|---|
| Event identity | `(table, primary key, LSN, op)`. Kafka offsets are not identity: after a Debezium restart the same change can arrive again with a new offset. |
| Duplicates | Bronze keeps them (immutable log). `silver.stg_*_events` is a ReplacingMergeTree keyed by the identity; views read with `FINAL`. Every dbt step is idempotent. |
| Ordering | Only per-key order matters. Same key: LSN order = commit order. Version = `(source_lsn, kafka_offset)`. One partition per topic keeps arrival order. Verified by `test_per_key_order_is_consistent_in_bronze`. |
| Update | The event with the highest version of a key wins (`argMax`). |
| Delete | Debezium sends a delete event (key only, placeholder values) and no tombstone (`tombstones.on.delete=false`). Silver flags `is_deleted`; current views and the fact hide the key; the history keeps the event. |
| Snapshot vs stream | Snapshot rows (`op = r`) share one LSN that is lower than any streamed change of the same key, so the same version rule applies. |
| Incremental read | Offset watermark per model, minus an overlap, plus an upper bound per run. |
| Schema change | Bronze stores the full JSON, so a new column cannot break ingestion. Silver lists columns explicitly (`installments` is already read from the payload; older events have NULL). With `ADD COLUMN ... DEFAULT x`, PostgreSQL does not rewrite old rows, so CDC never emits their new value: use a Debezium incremental snapshot (signal table) to backfill. |
| Retention | Kafka keeps 3 days. Bronze (ClickHouse volume, no TTL) is the long-term record. Incremental snapshots re-send only current rows: they cannot rebuild past versions or deleted rows. Back up bronze. |
| Isolation of planes | Ingestion (Debezium, Kafka, ClickHouse Kafka engine) does not depend on dbt. If the transformer stops, bronze keeps filling and silver/gold catch up later. |
