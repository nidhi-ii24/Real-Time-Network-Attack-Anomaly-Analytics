#!/bin/bash
# ==============================================================================
# Performance Benchmark: Block Size & Replication Factor Effects on Scalability
# Course: Big Data Analytics (26ECSC404) - KLE Technological University
# Aligns with PI-2.2.1.1: Examine replication & block size effects; document throughput.
# ==============================================================================

FILE="../../dataset/network_historical.csv"
HDFS_BASE="/network/replication_benchmark"

echo "=================================================================="
echo " 1. HDFS REPLICATION FACTOR EXPERIMENT"
echo "=================================================================="

# Upload file with replication factor 1
hdfs dfs -mkdir -p $HDFS_BASE
echo "[+] Uploading dataset with replication factor = 1..."
hdfs dfs -D dfs.replication=1 -put -f "$FILE" $HDFS_BASE/test_rep1.csv

echo "[+] Checking block status for rep=1:"
hdfs fsck $HDFS_BASE/test_rep1.csv -files -blocks -locations

echo -e "\n[+] Dynamically modifying replication factor to 2 using 'hdfs dfs -setrep'..."
hdfs dfs -setrep 2 $HDFS_BASE/test_rep1.csv

echo "[+] Checking block status after setrep to 2:"
hdfs fsck $HDFS_BASE/test_rep1.csv -files -blocks -locations

echo "------------------------------------------------------------------"
echo "Observation: On a single-node DataNode cluster, NameNode marks the"
echo "additional replica as 'Under-replicated' because no 2nd physical DataNode"
echo "exists to host the second copy."
echo "------------------------------------------------------------------"

echo -e "\n=================================================================="
echo " 2. HDFS BLOCK SIZE & THROUGHPUT EXPERIMENT"
echo "=================================================================="

# Test 1MB block size
echo "[+] Test A: 1 MB Block Size (dfs.blocksize=1048576)"
START_TIME=$(date +%s%N)
hdfs dfs -D dfs.blocksize=1048576 -put -f "$FILE" $HDFS_BASE/test_1mb_blocks.csv
END_TIME=$(date +%s%N)
DIFF_1MB=$(( (END_TIME - START_TIME) / 1000000 ))
echo "    Elapsed Time (1MB block size): ${DIFF_1MB} ms"
hdfs fsck $HDFS_BASE/test_1mb_blocks.csv -blocks | grep "Total blocks"

# Test 64MB block size (default/standard)
echo -e "\n[+] Test B: 64 MB Block Size (dfs.blocksize=67108864)"
START_TIME=$(date +%s%N)
hdfs dfs -D dfs.blocksize=67108864 -put -f "$FILE" $HDFS_BASE/test_64mb_blocks.csv
END_TIME=$(date +%s%N)
DIFF_64MB=$(( (END_TIME - START_TIME) / 1000000 ))
echo "    Elapsed Time (64MB block size): ${DIFF_64MB} ms"
hdfs fsck $HDFS_BASE/test_64mb_blocks.csv -blocks | grep "Total blocks"

echo -e "\n=================================================================="
echo " THROUGHPUT & SCALABILITY ANALYSIS SUMMARY (PI-2.2.1.1)"
echo "=================================================================="
echo "1. Small Block Size (1MB): Produces many small blocks, increasing NameNode"
echo "   metadata memory overhead and causing higher RPC/IO latency."
echo "2. Large Block Size (64MB/128MB): Produces fewer blocks, sequential disk"
echo "   streaming, reduced NameNode burden, and superior MapReduce throughput."
echo "=================================================================="
