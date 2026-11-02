{#
  HMAC-SHA256 with a secret key, built from SHA256 (ClickHouse has no native HMAC):
      HMAC(K, m) = SHA256( (K xor opad) || SHA256( (K xor ipad) || m ) )
  The two padded key blocks live in ops.pii_pads (written by the scheduler from the secret key file) and are read
  with scalar subqueries, so the key material never appears in the SQL text, dbt logs or compiled files.
  Empty input stays empty. Output: 64 lowercase hex characters, identical to Python's hmac.new(key, m, sha256).hexdigest().
#}
{% macro hmac_sha256(expr) -%}
if(length({{ expr }}) = 0, '',
   lower(hex(SHA256(concat(
       (SELECT unhex(opad_hex) FROM ops.pii_pads LIMIT 1),
       SHA256(concat((SELECT unhex(ipad_hex) FROM ops.pii_pads LIMIT 1), {{ expr }})))))))
{%- endmacro %}

{# Display-safe mobile number: keep the first 4 and last 2 digits. #}
{% macro mask_mobile(expr) -%}
if(length({{ expr }}) = 11, concat(substring({{ expr }}, 1, 4), '*****', substring({{ expr }}, 10, 2)), '')
{%- endmacro %}
