# Metabase + the ClickHouse driver baked into the image.
# (A bind-mounted /plugins folder owned by the host user is not writable by Metabase's own user, so the
# driver is never loaded. Baking the jar in avoids host permission problems on any machine.)
ARG HUB_REGISTRY=docker.io
ARG METABASE_VERSION=v0.53.5
FROM ${HUB_REGISTRY}/metabase/metabase:${METABASE_VERSION}
# driver minor version must match the Metabase minor version (0.53.x <-> 1.53.x)
COPY plugins/clickhouse.metabase-driver.jar /plugins/clickhouse.metabase-driver.jar
