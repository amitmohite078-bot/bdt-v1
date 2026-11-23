#!/usr/bin/env bash
# SmartProtein Stage 5: Batch HDFS Ingestion via hdfs dfs -put
# Loads raw CSV datasets and label images into /smartprotein/raw/ and repairs Hive partitions.

set -euo pipefail

INGEST_DATE="${1:-$(date -u +%Y-%m-%d)}"
LOCAL_RAW="/opt/smartprotein/data/raw"

echo "[1/4] Ensuring HDFS raw partition directories exist for ${INGEST_DATE}..."
docker exec sp-namenode hdfs dfs -mkdir -p "/smartprotein/raw/food_composition/ingestion_date=${INGEST_DATE}"
docker exec sp-namenode hdfs dfs -mkdir -p "/smartprotein/raw/store_prices/ingestion_date=${INGEST_DATE}"
docker exec sp-namenode hdfs dfs -mkdir -p "/smartprotein/raw/user_wearables/ingestion_date=${INGEST_DATE}"
docker exec sp-namenode hdfs dfs -mkdir -p "/smartprotein/raw/store_locations"
docker exec sp-namenode hdfs dfs -mkdir -p "/smartprotein/raw/crowd_flags"
docker exec sp-namenode hdfs dfs -mkdir -p "/smartprotein/raw/label_images"

echo "[2/4] Pushing raw multi-source datasets into Hadoop HDFS..."
docker exec sp-namenode hdfs dfs -put -f \
  "${LOCAL_RAW}/food_composition/ingestion_date=${INGEST_DATE}/"*.csv \
  "/smartprotein/raw/food_composition/ingestion_date=${INGEST_DATE}/"

docker exec sp-namenode hdfs dfs -put -f \
  "${LOCAL_RAW}/store_locations/store_locations.csv" \
  "/smartprotein/raw/store_locations/"

docker exec sp-namenode hdfs dfs -put -f \
  "${LOCAL_RAW}/store_prices/ingestion_date=${INGEST_DATE}/"*.csv \
  "/smartprotein/raw/store_prices/ingestion_date=${INGEST_DATE}/"

docker exec sp-namenode hdfs dfs -put -f \
  "${LOCAL_RAW}/user_wearables/ingestion_date=${INGEST_DATE}/"*.csv \
  "/smartprotein/raw/user_wearables/ingestion_date=${INGEST_DATE}/"

docker exec sp-namenode hdfs dfs -put -f \
  "${LOCAL_RAW}/crowd_flags/crowd_flags.csv" \
  "/smartprotein/raw/crowd_flags/"

echo "[3/4] Checking HDFS block allocation & file sizes..."
docker exec sp-namenode hdfs dfs -du -h /smartprotein/raw/

echo "[4/4] Registering new partitions in Hive Metastore..."
docker exec sp-hive-server beeline -u jdbc:hive2://localhost:10000 -e "
  MSCK REPAIR TABLE smartprotein_raw.raw_food_composition;
  MSCK REPAIR TABLE smartprotein_raw.raw_store_prices;
  MSCK REPAIR TABLE smartprotein_raw.raw_user_wearables;
"

echo "[Done] Batch ingestion complete for ingestion_date=${INGEST_DATE}."

