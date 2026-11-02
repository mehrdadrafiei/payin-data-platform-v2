#!/bin/sh
# dbt with the project paths used by the scheduler:  dbtw run | test | source freshness | docs generate ...
cmd="$1"; shift
exec dbt "$cmd" --project-dir /app/dbt --profiles-dir /app/dbt --target-path /tmp/dbt/target --log-path /tmp/dbt/logs "$@"
