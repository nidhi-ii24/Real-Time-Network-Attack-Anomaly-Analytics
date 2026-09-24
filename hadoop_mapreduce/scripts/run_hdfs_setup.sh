#!/bin/bash
# ==============================================================================
# HDFS Setup Script for BDA Network Analytics Project
# Course: Big Data Analytics (26ECSC404) - KLE Technological University
# ==============================================================================

set -e

echo "=== STEP 1: Verifying HDFS Daemons ==="
jps | grep -E "NameNode|DataNode" || {
    echo "[-] Error: Hadoop HDFS Daemons (NameNode/DataNode) are not running."
    echo "    Run 'start-dfs.sh' first."
    exit 1
}

echo "[+] HDFS Daemons active."

echo -e "\n=== STEP 2: Creating HDFS Directory Hierarchy ==="
hdfs dfs -mkdir -p /network/raw
hdfs dfs -mkdir -p /network/input
hdfs dfs -mkdir -p /network/output
hdfs dfs -mkdir -p /network/hive_warehouse
hdfs dfs -mkdir -p /network/replication_demo

echo -e "\n=== STEP 3: Uploading Historical Network Flow Dataset ==="
DATASET_LOCAL="../../dataset/network_historical.csv"

if [ -f "$DATASET_LOCAL" ]; then
    echo "[+] Uploading $DATASET_LOCAL to /network/raw/ ..."
    hdfs dfs -put -f "$DATASET_LOCAL" /network/raw/
else
    echo "[-] Warning: $DATASET_LOCAL not found locally. Please run python dataset/generate_dataset.py"
fi

echo -e "\n=== STEP 4: Listing HDFS Files & Disk Usage ==="
hdfs dfs -ls -h /network/raw
hdfs dfs -du -h /network/raw

echo -e "\n=== HDFS Setup Complete ==="
