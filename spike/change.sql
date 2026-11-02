-- Streaming changes after the snapshot
-- (a) ONE transaction touching 3 rows -> do they get different LSNs?
BEGIN;
UPDATE core.transactions SET status = 'REVERSED', updated_at = now() WHERE transaction_id IN (1, 2, 3);
COMMIT;
-- (b) single update
UPDATE core.transactions SET amount = amount + 1, updated_at = now() WHERE transaction_id = 10;
-- (c) hard delete
DELETE FROM core.transactions WHERE transaction_id = 20;
