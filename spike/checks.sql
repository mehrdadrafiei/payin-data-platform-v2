SELECT '1a. events by op (expect r=1000, u=4, d=1)' AS step;
SELECT op, count() AS events FROM bronze.transactions_cdc GROUP BY op ORDER BY op;

SELECT '1b. rows in error table (expect 0)' AS step;
SELECT count() AS errors FROM bronze.cdc_errors;

SELECT '1c. delete event payload (expect after=null; before has the key, other fields null)' AS step;
SELECT payload FROM bronze.transactions_cdc WHERE op = 'd' FORMAT Vertical;

SELECT '2a. updates of ids 1,2,3 made in ONE Postgres transaction: same or different LSN?' AS step;
SELECT pk, source_lsn, _offset FROM bronze.transactions_cdc WHERE op = 'u' AND pk IN (1, 2, 3) ORDER BY _offset;

SELECT '2b. snapshot rows: how many distinct LSNs?' AS step;
SELECT uniqExact(source_lsn) AS distinct_lsn, count() AS snapshot_rows FROM bronze.transactions_cdc WHERE op = 'r';

SELECT '2c. duplicate event identities (pk, lsn, op) (expect 0 rows)' AS step;
SELECT pk, source_lsn, op, count() AS c FROM bronze.transactions_cdc GROUP BY pk, source_lsn, op HAVING c > 1;

SELECT '2d. per-key: Kafka offset order equals LSN order (expect 0 rows)' AS step;
SELECT pk FROM
(
    SELECT pk, groupArray(source_lsn) AS l, arraySort(groupArray(source_lsn)) AS ls
    FROM (SELECT pk, source_lsn FROM bronze.transactions_cdc ORDER BY _offset)
    GROUP BY pk
)
WHERE l != ls
SETTINGS max_threads = 1;
