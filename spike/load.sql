-- Spike data: 3 merchants + 1000 transactions (loaded BEFORE the connector, so the snapshot has something to read)
INSERT INTO core.merchants (merchant_id, name, mcc, province, risk_level, owner_national_id, owner_mobile, updated_at) VALUES
 (1, 'Shop A', '5411', 'Tehran',  'LOW',    '0012345678', '09121234567', now() - interval '181 days'),
 (2, 'Shop B', '5812', 'Fars',    'MEDIUM', '0023456789', '09131234567', now() - interval '181 days'),
 (3, 'Shop C', '5541', 'Isfahan', 'HIGH',   '0034567890', '09141234567', now() - interval '181 days');

INSERT INTO core.transactions (merchant_id, channel, card_bin, amount, fee_amount, status, created_at, updated_at)
SELECT 1 + g % 3,
       (ARRAY['POS','IPG','QR'])[1 + g % 3],
       '603799',
       100000 + g * 1000,
       1000,
       'SUCCESS',
       now() - (g || ' minutes')::interval,
       now() - (g || ' minutes')::interval
FROM generate_series(1, 1000) AS g;

SELECT count(*) AS transactions_loaded FROM core.transactions;
