-- Base data for the simulated payment company. SYNTHETIC: every value below is generated.
-- Parameters (psql variables): :n_txn  :n_merchants
\set ON_ERROR_STOP on
SET synchronous_commit = off;
SET work_mem = '64MB';
SET maintenance_work_mem = '256MB';
SELECT setseed(0.42);                      -- reproducible data

-- a re-run after a failed attempt starts clean (CDC is not attached yet at this point)
DROP INDEX IF EXISTS core.idx_transactions_merchant_id;   -- also makes the bulk load faster
DROP INDEX IF EXISTS core.idx_transactions_created_at;
TRUNCATE core.transactions, core.merchants RESTART IDENTITY;

SELECT now() AS seed_ts \gset

-- Merchants: initial state is dated 181 days ago (before the first transaction).
-- ~25% of merchants share an owner (same national id / mobile).
INSERT INTO core.merchants (merchant_id, name, mcc, province, risk_level,
                            owner_national_id, owner_mobile, updated_at)
SELECT g,
       'Merchant ' || g,
       (ARRAY['5411','5812','5541','5311','5732','5912','4121','5499','5651','7399','5200','5814'])[1 + (g % 12)],
       (ARRAY['Tehran','Isfahan','Fars','Khorasan Razavi','East Azerbaijan','Khuzestan','Mazandaran',
              'Alborz','Gilan','Kerman','Qom','Yazd','Hormozgan','Kermanshah','West Azerbaijan'])[1 + floor(r * 15)::int % 15],
       CASE WHEN r2 < 0.70 THEN 'LOW' WHEN r2 < 0.95 THEN 'MEDIUM' ELSE 'HIGH' END,
       lpad(((((g % 1500) + 1)::bigint * 6700417) % 10000000000)::text, 10, '0'),
       '09' || lpad(((((g % 1500) + 1)::bigint * 7919) % 1000000000)::text, 9, '0'),
       :'seed_ts'::timestamptz - interval '181 days'
FROM (SELECT g, random() AS r, random() AS r2 FROM generate_series(1, :n_merchants) g) s;

-- Transactions: time moves forward with the id (id order ~ time order) over 180 days.
-- Status mix 80/10/10 (SUCCESS/FAILED/REVERSED); merchant popularity is skewed so "top 10" is meaningful.
INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status, created_at, updated_at)
SELECT merchant_id, channel, card_bin, amount,
       CASE WHEN status = 'FAILED' THEN 0 ELSE (amount * 0.006)::bigint END,
       status,
       created_at,
       LEAST(created_at + CASE WHEN status = 'REVERSED' THEN random() * interval '3 hours'
                               ELSE random() * interval '10 seconds' END,
             :'seed_ts'::timestamptz)
FROM (
    SELECT
        1 + floor(:n_merchants * power(random(), 2.0))::int AS merchant_id,
        CASE WHEN c < 0.55 THEN 'POS' WHEN c < 0.85 THEN 'IPG' ELSE 'QR' END AS channel,
        (ARRAY['603799','610433','589463','627353','621986','622106','639346','636214','603770','589210'])[1 + floor(random() * 10)::int % 10] AS card_bin,
        (round(50000 * exp(random() * 5) / 1000) * 1000)::bigint AS amount,
        CASE WHEN s < 0.80 THEN 'SUCCESS' WHEN s < 0.90 THEN 'FAILED' ELSE 'REVERSED' END AS status,
        :'seed_ts'::timestamptz - interval '180 days'
            + ((g::float8 - 1 + random()) / :n_txn) * interval '180 days' AS created_at
    FROM (SELECT g, random() AS c, random() AS s FROM generate_series(1, :n_txn) g) x
) y;

-- A normal OLTP index set (primary key + foreign key + time). The baseline is slow because of the
-- aggregations over millions of rows, not because indexes were left out on purpose.
CREATE INDEX idx_transactions_merchant_id ON core.transactions (merchant_id);
CREATE INDEX idx_transactions_created_at  ON core.transactions (created_at);
ANALYZE core.merchants;
ANALYZE core.transactions;

-- Sanity report (shows up in the seed container log)
SELECT status, count(*) AS txns, round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM core.transactions GROUP BY status ORDER BY status;
SELECT min(created_at) AS first_txn, max(created_at) AS last_txn, count(*) AS total FROM core.transactions;

INSERT INTO public._seed_state (key, value) VALUES ('seed_ts', :'seed_ts'), ('base_done', '1');
