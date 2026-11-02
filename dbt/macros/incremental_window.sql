{#
  Which offsets does this run read?
    first run   : the first max_batch_events offsets of the source
    later runs  : from (own watermark - overlap) up to max_batch_events further
  The watermark is max(<watermark_col>) already stored in {{ this }}. Offsets of one Kafka topic are a total order
  (one partition), so no timestamps are involved. The overlap re-reads the most recent offsets on purpose:
  everything downstream is idempotent, so re-reading is harmless and protects against rows that became visible late.
#}
{% macro offset_window(offset_col, source_relation, watermark_col='kafka_offset') -%}
    {%- if is_incremental() -%}
        {{ offset_col }} >= (SELECT toUInt64(greatest(toInt64(max({{ watermark_col }})) - {{ var('overlap_offsets') }}, 0)) FROM {{ this }})
    AND {{ offset_col }} <  (SELECT toUInt64(greatest(toInt64(max({{ watermark_col }})) - {{ var('overlap_offsets') }}, 0)) FROM {{ this }}) + {{ var('max_batch_events') }}
    {%- else -%}
        {{ offset_col }} < (SELECT min({{ offset_col }}) FROM {{ source_relation }}) + {{ var('max_batch_events') }}
    {%- endif -%}
{%- endmacro %}

{# Version stamp of one run: microseconds since epoch, but always larger than every version already stored. #}
{% macro run_version() -%}
    {%- if is_incremental() -%}
    (SELECT greatest(toUInt64(toUnixTimestamp64Micro(now64(6))), max(_version) + 1) FROM {{ this }})
    {%- else -%}
    toUInt64(toUnixTimestamp64Micro(now64(6)))
    {%- endif -%}
{%- endmacro %}
