#!/usr/bin/env bash
set -euo pipefail

echo "[SmartProtein] Creating 5-Zone HDFS Lakehouse Directories..."
hdfs dfs -mkdir -p /smartprotein/raw/food_composition
hdfs dfs -mkdir -p /smartprotein/raw/store_locations
hdfs dfs -mkdir -p /smartprotein/raw/store_prices
hdfs dfs -mkdir -p /smartprotein/raw/user_wearables
hdfs dfs -mkdir -p /smartprotein/raw/label_images
hdfs dfs -mkdir -p /smartprotein/raw/crowd_flags

hdfs dfs -mkdir -p /smartprotein/staging
hdfs dfs -mkdir -p /smartprotein/curated
hdfs dfs -mkdir -p /smartprotein/rejected/all_records
hdfs dfs -mkdir -p /smartprotein/analytics/dq_scorecards
hdfs dfs -mkdir -p /smartprotein/analytics/dq_lineage
hdfs dfs -mkdir -p /smartprotein/analytics/user_recommendations
hdfs dfs -mkdir -p /smartprotein/analytics/realtime_price_alerts

hdfs dfs -chmod -R 777 /smartprotein

echo "[SmartProtein] HDFS Zone Layout:"
hdfs dfs -ls /smartprotein

