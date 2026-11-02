{#
  Incremental aggregate over the fact table, recomputed only for the periods (days or months) in which fact rows
  changed since the previous run.
    fct         : the versioned fact table (ref to fct_transactions_state)
    period_col  : 'txn_date' or 'txn_month' - the unit that is recomputed as a whole
    keys        : grouping columns (must include period_col)
    measures    : [[name, expression], ...]
  Steps: (1) affected periods = periods of fact rows with _version newer than the last run saw;
         (2) recompute all groups of those periods from the fact (FINAL = latest version, deleted rows hidden);
         (3) groups that existed before but vanished get a zero row, so they are overwritten, not left stale.
  The table is a ReplacingMergeTree(_version): the newest row of a key wins. Read it through the view of the same
  name without the _state suffix (it uses FINAL and hides all-zero groups).
#}
{% macro incremental_aggregate(fct, period_col, keys, measures) -%}
{%- set key_list = keys | join(', ') -%}
WITH affected AS (
    SELECT DISTINCT {{ period_col }} AS p
    FROM {{ fct }}
    WHERE _version > {% if is_incremental() %}(SELECT coalesce(max(_fct_version_seen), 0) FROM {{ this }}){% else %}0{% endif %}
),
recomputed AS (
    SELECT
        {{ key_list }},
        {% for m in measures -%}
        {{ m[1] }} AS {{ m[0] }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ fct }} FINAL
    WHERE {{ period_col }} IN (SELECT p FROM affected)
    GROUP BY {{ key_list }}
){% if is_incremental() %},
stale AS (
    SELECT
        {{ key_list }},
        {% for m in measures -%}
        0 AS {{ m[0] }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ this }} FINAL
    WHERE {{ period_col }} IN (SELECT p FROM affected)
      AND ({{ key_list }}) NOT IN (SELECT {{ key_list }} FROM recomputed)
){% endif %},
merged AS (
    SELECT * FROM recomputed
    {% if is_incremental() -%}
    UNION ALL
    SELECT * FROM stale
    {% endif %}
)
SELECT
    {{ key_list }},
    {% for m in measures -%}
    {{ m[0] }},
    {% endfor %}
    {{ run_version() }} AS _version,
    (SELECT coalesce(max(_version), 0) FROM {{ fct }}) AS _fct_version_seen
FROM merged
{%- endmacro %}
