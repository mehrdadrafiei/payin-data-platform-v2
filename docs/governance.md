# Data governance

This is a local demonstration using synthetic data, not a production security or
regulatory-compliance design. The listed owner is the project team
(`payments-data-platform`); source data is owned by the simulated payments
operation. No real data subjects or business owners are represented.

## Data ownership and sensitivity

| Data set | Owner | Sensitive fields / classification | Access and handling |
|---|---|---|---|
| `core.merchants` | Simulated payments operation | `owner_national_id`, `owner_mobile`: direct identifiers; `name`, `merchant_id`, `province`, `risk_level`: business/confidential | Source of truth. The CDC account reads the table for replication. The dashboard's PostgreSQL account has column-level access that excludes the two direct identifiers. |
| `core.transactions` | Simulated payments operation | `transaction_id`, `merchant_id`, `amount`, `fee_amount`, `status`, timestamps: transaction/business data; `card_bin`: issuer-identifying prefix, not a full card number | Source of truth. The dashboard's PostgreSQL account can read the table but is read-only. No full PAN or cardholder data is generated. |
| `bronze.merchants_cdc`, `bronze.transactions_cdc` | `payments-data-platform` (derived copy) | Full CDC envelope, including source merchant identifiers and raw owner national ID/mobile | Append-only replication landing zone. This is the only analytical layer intended to retain raw owner identifiers. Restrict it to ingestion and platform administrators; do not expose it to dashboards. No TTL is configured, so its storage must be treated as sensitive for its full lifetime. |
| `bronze.cdc_errors` | `payments-data-platform` (operational log) | `raw_message` may contain an unparseable source payload, including identifiers | Restrict like bronze; inspect and clean up only through an explicit retention/incident process. A zero-error test does not make this table safe for broad access. |
| `silver.*` merchant event/current-state models | `payments-data-platform` | HMAC tokens are pseudonymised personal data; `owner_mobile_masked` is masked personal data. Merchant and transaction attributes remain business data. | Raw identifiers are replaced during parsing. Tokens remain linkable (the same input produces the same token), so they are not anonymous data. Keep silver access limited to transformation operators. |
| `gold.dim_merchant` | `payments-data-platform` | Includes `owner_national_id_token`, a linkable pseudonymous identifier; other merchant attributes are business data | The BI account can read `gold.*`, including this dimension. The token must still be handled as sensitive data; it is not needed by the dashboard cards and should not be treated as anonymized. |
| `gold.fct_transactions` and `gold.agg_*` | `payments-data-platform` | Fact contains transaction/business data and `card_bin`; aggregates contain business metrics | The BI account reads gold for dashboards. Facts and aggregates omit direct owner identifiers and HMAC tokens. |
| `ops.pii_pads` and `/secrets/pii_hmac_key` | `payments-data-platform` | HMAC key material / derived key pads | Transformation service only. The key is generated into the Docker `secrets` volume, not committed to Git. Protect and back up that volume carefully; losing the key prevents consistent token generation. |

## Identity, access, and retention

- `cdc` is a replication/read account, not a dashboard account. `bi_ro` is the
  read-only Metabase account: PostgreSQL grants it access to transactions and
  selected non-PII merchant columns; ClickHouse grants it `SELECT` on `gold.*`.
- The analytical layers should not be read with the ClickHouse administrator
  credentials by dashboards. Bronze, silver, the key pads, and source PII are
  outside the intended dashboard access path.
- Bronze is append-only and has no TTL. Kafka is configured for three-day
  retention, but it is not a deletion policy for the bronze copy. Removing
  source test transactions creates CDC delete events; it does not erase their
  earlier events from bronze.
- Demo passwords and the local admin credential are intentionally static for
  this synthetic take-home environment. They are not suitable for deployment
  on a shared or publicly reachable host. Compose publishes service ports on
  localhost, but host access still needs to be trusted.

## Limitations

This project does not implement production secret management, key rotation,
automated data-subject deletion, formal retention enforcement, audit alerting,
or a privacy/compliance review. In particular, a keyed HMAC is pseudonymisation,
not anonymisation, and raw CDC records remain in bronze unless an operator
removes the data or the volume is destroyed.
