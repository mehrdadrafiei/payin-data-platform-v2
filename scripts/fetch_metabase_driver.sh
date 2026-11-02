#!/usr/bin/env bash
# Downloads the ClickHouse driver for Metabase (needs GitHub access once). Minor version must match Metabase's.
set -euo pipefail
VER="${CH_DRIVER_VERSION:-1.53.2}"
DEST="metabase/plugins/clickhouse.metabase-driver.jar"
mkdir -p metabase/plugins
if [ -s "$DEST" ]; then echo "[driver] ClickHouse driver already present"; exit 0; fi
URL="https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/${VER}/clickhouse.metabase-driver.jar"
echo "[driver] downloading ${URL}"
curl -fL --retry 3 --connect-timeout 20 -o "${DEST}.tmp" "$URL"
mv "${DEST}.tmp" "$DEST"
echo "[driver] saved to ${DEST}"
