#!/usr/bin/env bash
set -euo pipefail

echo "[1/4] Creating HDFS Medallion directories..."
docker exec fitguard-namenode hdfs dfs -mkdir -p /fitguard/bronze/user_profiles
docker exec fitguard-namenode hdfs dfs -mkdir -p /fitguard/bronze/protein_foods_catalog
docker exec fitguard-namenode hdfs dfs -mkdir -p /fitguard/bronze/wearable_telemetry
docker exec fitguard-namenode hdfs dfs -mkdir -p /fitguard/silver
docker exec fitguard-namenode hdfs dfs -mkdir -p /fitguard/quarantine
docker exec fitguard-namenode hdfs dfs -mkdir -p /fitguard/gold

echo "[2/4] Uploading raw Bronze datasets to Hadoop HDFS..."
docker exec fitguard-namenode hdfs dfs -put -f /data/bronze/user_profiles/user_profiles.csv /fitguard/bronze/user_profiles/
docker exec fitguard-namenode hdfs dfs -put -f /data/bronze/protein_foods_catalog/protein_foods_catalog.csv /fitguard/bronze/protein_foods_catalog/
docker exec fitguard-namenode hdfs dfs -put -f /data/bronze/wearable_telemetry/*.jsonl /fitguard/bronze/wearable_telemetry/

echo "[3/4] Inspecting HDFS block layout & file count..."
docker exec fitguard-namenode hdfs dfs -ls -h /fitguard/bronze/wearable_telemetry | head -n 10

echo "[4/4] Submitting PySpark Medallion DQ Pipeline to Spark Master..."
docker exec fitguard-spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark_jobs/spark_medallion_pipeline.py \
  --base-path hdfs://namenode:9000/fitguard \
  --namenode-uri hdfs://namenode:9000

echo "[Done] Verify HDFS Silver/Quarantine/Gold outputs:"
docker exec fitguard-namenode hdfs dfs -ls -R /fitguard/silver /fitguard/quarantine /fitguard/gold

