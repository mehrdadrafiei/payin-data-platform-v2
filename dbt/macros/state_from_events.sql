{#
  Current state of every key from its change events (silver "state" tables).
  For each key in the batch the LATEST event wins; version = (source_lsn, kafka_offset). For one key, LSN order is
  commit order (two transactions cannot change the same row at the same time).
  A DELETE event only carries the key (all other fields are placeholders), so a deleted key keeps its last known
  real values (from last_known) and is flagged is_deleted = 1.
  NOTE on names: the intermediate aggregates are called v_lsn / v_offset / v_deleted on purpose. ClickHouse resolves
  an alias used inside another aggregate to the aggregate itself, which is an error (ILLEGAL_AGGREGATION).
#}
{% macro state_from_events(events, key, cols) -%}
WITH batch AS (
    SELECT * FROM {{ events }}
    WHERE {{ offset_window('kafka_offset', events) }}
),
latest AS (
    SELECT
        {{ key }},
        max(source_lsn) AS v_lsn,
        max(kafka_offset) AS v_offset,
        argMax(is_deleted, (source_lsn, kafka_offset)) AS v_deleted,
        {% for c in cols -%}
        argMaxIf({{ c }}, (source_lsn, kafka_offset), is_deleted = 0) AS {{ c }}{{ "," if not loop.last }}
        {% endfor %}
    FROM batch
    GROUP BY {{ key }}
),
last_known AS (
    SELECT
        {{ key }},
        {% for c in cols -%}
        argMaxIf({{ c }}, (source_lsn, kafka_offset), is_deleted = 0) AS {{ c }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ events }}
    WHERE {{ key }} IN (SELECT {{ key }} FROM latest WHERE v_deleted = 1)
    GROUP BY {{ key }}
)
SELECT
    l.{{ key }} AS {{ key }},
    l.v_lsn AS source_lsn,
    l.v_offset AS kafka_offset,
    l.v_deleted AS is_deleted,
    {% for c in cols -%}
    if(l.v_deleted = 1, k.{{ c }}, l.{{ c }}) AS {{ c }}{{ "," if not loop.last }}
    {% endfor %}
FROM latest AS l
LEFT JOIN last_known AS k ON k.{{ key }} = l.{{ key }}
{%- endmacro %}
