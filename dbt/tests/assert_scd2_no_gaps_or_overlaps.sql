{# Every version must end exactly where the next one starts, and have a positive length. Returns the offenders. #}
SELECT merchant_id, version_no, valid_from, valid_to, next_from
FROM (
    SELECT
        merchant_id, version_no, valid_from, valid_to,
        leadInFrame(valid_from, 1, toDateTime64('2299-12-31 00:00:00', 6, 'UTC')) OVER (
            PARTITION BY merchant_id ORDER BY version_no
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_from
    FROM {{ ref('dim_merchant') }}
)
WHERE valid_to != next_from OR valid_from >= valid_to
