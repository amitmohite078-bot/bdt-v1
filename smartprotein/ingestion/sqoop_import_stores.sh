#!/usr/bin/env bash
# SmartProtein Stage 5: Apache Sqoop Import from PostgreSQL OLTP -> Hadoop HDFS Raw Zone
# Requires Sqoop client + PostgreSQL JDBC driver (postgresql-42.x.jar) on the Sqoop node.

set -euo pipefail

INGEST_DATE="${1:-$(date -u +%Y-%m-%d)}"
JDBC_URL="jdbc:postgresql://postgres-stores:5432/smartprotein_oltp"
TARGET_PRICES_DIR="/smartprotein/raw/store_prices/sqoop_import_${INGEST_DATE}"
TARGET_LOC_DIR="/smartprotein/raw/store_locations/sqoop_import"

echo "[Sqoop] Importing store_locations (single mapper reference table)..."
sqoop import \
  --connect "${JDBC_URL}" \
  --username sp_user \
  --password sp_pass \
  --table store_locations \
  --target-dir "${TARGET_LOC_DIR}" \
  --delete-target-dir \
  --num-mappers 1 \
  --fields-terminated-by ','

echo "[Sqoop] Importing store_prices with 4 parallel mappers split by offer_id..."
sqoop import \
  --connect "${JDBC_URL}" \
  --username sp_user \
  --password sp_pass \
  --table store_prices \
  --split-by offer_id \
  --num-mappers 4 \
  --target-dir "${TARGET_PRICES_DIR}" \
  --delete-target-dir \
  --fields-terminated-by ',' \
  --as-textfile

echo "[Sqoop] Verifying HDFS import output..."
hdfs dfs -ls -h "${TARGET_PRICES_DIR}"

