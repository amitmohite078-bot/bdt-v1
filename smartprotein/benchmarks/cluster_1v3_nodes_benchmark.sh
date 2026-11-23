#!/usr/bin/env bash
# SmartProtein Stage 12: Cluster 1-Node vs. 3-Node HDFS + Spark Scaling Experiment
# Uses the Docker Compose 'scale3' profile defined in smartprotein/docker/docker-compose.yml.

set -euo pipefail

COMPOSE_FILE="smartprotein/docker/docker-compose.yml"

echo "[1/4] Generating 1-Million-Row Benchmark Dataset..."
python3 smartprotein/data_generator/generate_smartprotein_data.py --scale 1m

echo "[2/4] Experiment A: Running on 1-DataNode HDFS Cluster..."
docker compose -f "${COMPOSE_FILE}" up -d
sleep 25
bash smartprotein/ingestion/ingest_to_hdfs.sh
TIME_1NODE=$(/usr/bin/time -f "%e" docker exec sp-spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/smartprotein/spark_jobs/cleaning/clean_and_quarantine.py 2>&1 | tail -n 1)
echo "1-DataNode Execution Time (s): ${TIME_1NODE}"

echo "[3/4] Experiment B: Scaling HDFS to 3 DataNodes (datanode1 + datanode2 + datanode3)..."
docker compose -f "${COMPOSE_FILE}" --profile scale3 up -d
sleep 20
docker exec sp-namenode hdfs dfs -setrep -R 3 /smartprotein/raw/
TIME_3NODE=$(/usr/bin/time -f "%e" docker exec sp-spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/smartprotein/spark_jobs/cleaning/clean_and_quarantine.py 2>&1 | tail -n 1)
echo "3-DataNode Execution Time (s): ${TIME_3NODE}"

echo "[4/4] Record both timings in your Stage 12 evaluation table!"

