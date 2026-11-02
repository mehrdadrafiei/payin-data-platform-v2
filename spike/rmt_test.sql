-- Check 3: ReplacingMergeTree with is_deleted + FINAL in this ClickHouse version
DROP TABLE IF EXISTS silver.rmt_test;
CREATE TABLE silver.rmt_test (id Int64, ver UInt64, is_deleted UInt8, val String)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY id;
INSERT INTO silver.rmt_test VALUES (1, 1, 0, 'a'), (2, 1, 0, 'b');
INSERT INTO silver.rmt_test VALUES (1, 2, 1, 'a');     -- delete id 1
INSERT INTO silver.rmt_test VALUES (2, 2, 0, 'b2');    -- update id 2
SELECT 'without FINAL (old versions may still be visible)' AS step, count() AS rows FROM silver.rmt_test;
SELECT 'with FINAL (expect ONLY id=2, val=b2)' AS step, id, val FROM silver.rmt_test FINAL;
DROP TABLE silver.rmt_test;
