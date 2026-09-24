#!/bin/bash
# ==============================================================================
# Complete BDA Pipeline Runner for Hadoop on WSL
# Course: Big Data Analytics (26ECSC404) - KLE Technological University
# Run inside your Ubuntu WSL terminal: bash wsl_run_pipeline.sh
# ==============================================================================

set -e

# Load environment
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$JAVA_HOME/bin

# Locate project directory inside WSL mount
if [ -d "/mnt/d/7th sem/BDA/BDA Project" ]; then
    PROJECT_DIR="/mnt/d/7th sem/BDA/BDA Project"
elif [ -d "/mnt/host/d/7th sem/BDA/BDA Project" ]; then
    PROJECT_DIR="/mnt/host/d/7th sem/BDA/BDA Project"
else
    PROJECT_DIR="$(pwd)"
fi

cd "$PROJECT_DIR"
echo "[+] Working inside project directory: $PROJECT_DIR"

echo -e "\n=================================================================="
echo " STEP 1: VERIFYING HADOOP DAEMONS"
echo "=================================================================="
jps | grep -E "NameNode|DataNode|ResourceManager" || {
    echo "[-] Error: Hadoop daemons not running. Starting them now..."
    $HADOOP_HOME/sbin/start-dfs.sh
    $HADOOP_HOME/sbin/start-yarn.sh
}
echo "[+] Hadoop daemons active:"
jps

echo -e "\n=================================================================="
echo " STEP 2: CREATING HDFS DIRECTORY HIERARCHY"
echo "=================================================================="
hdfs dfs -mkdir -p /network/raw
hdfs dfs -mkdir -p /network/output
hdfs dfs -mkdir -p /network/replication_demo

echo -e "\n=================================================================="
echo " STEP 3: INGESTING HISTORICAL DATASET INTO HDFS"
echo "=================================================================="
# Ensure dataset exists
if [ ! -f "dataset/network_historical.csv" ]; then
    echo "[+] Generating historical network flow dataset..."
    python3 dataset/generate_dataset.py 25000
fi

echo "[+] Uploading dataset/network_historical.csv to HDFS (/network/raw/)..."
hdfs dfs -put -f dataset/network_historical.csv /network/raw/
hdfs dfs -ls -h /network/raw/
echo "[+] HDFS Disk Usage:"
hdfs dfs -du -h /network/raw/

echo -e "\n=================================================================="
echo " STEP 4: COMPILING & PACKAGING HADOOP MAPREDUCE JOBS"
echo "=================================================================="
cd hadoop_mapreduce
if command -v mvn &> /dev/null; then
    echo "[+] Building MapReduce JAR via Maven..."
    mvn clean package -DskipTests -q
else
    echo "[+] Maven not installed. Compiling directly with javac..."
    mkdir -p target/classes
    javac -cp $($HADOOP_HOME/bin/hadoop classpath) -d target/classes src/main/java/bda/network/*.java
    jar -cvf target/network-analytics-mapreduce-1.0-SNAPSHOT.jar -C target/classes .
fi
cd ..

JAR_PATH="hadoop_mapreduce/target/network-analytics-mapreduce-1.0-SNAPSHOT.jar"
echo "[+] Compiled JAR ready: $JAR_PATH"

echo -e "\n=================================================================="
echo " STEP 5: RUNNING MAPREDUCE JOB 1 (PROTOCOL DISTRIBUTION)"
echo "=================================================================="
hdfs dfs -rm -r -f /network/output/protocol_counts
hadoop jar "$JAR_PATH" bda.network.ProtocolCountJob /network/raw/network_historical.csv /network/output/protocol_counts

echo -e "\n--- MAPREDUCE 1 RESULTS (Protocol Frequency) ---"
hdfs dfs -cat /network/output/protocol_counts/part-r-00000

echo -e "\n=================================================================="
echo " STEP 6: RUNNING MAPREDUCE JOB 2 (TOP SOURCE IPS - 'TOP TALKERS')"
echo "=================================================================="
hdfs dfs -rm -r -f /network/output/top_ips
hadoop jar "$JAR_PATH" bda.network.TopSourceIPJob /network/raw/network_historical.csv /network/output/top_ips

echo -e "\n--- MAPREDUCE 2 RESULTS (Top 10 Source IPs) ---"
hdfs dfs -cat /network/output/top_ips/part-r-00000 | sort -k2 -nr | head -n 10

echo -e "\n=================================================================="
echo " STEP 7: RUNNING MAPREDUCE JOB 3 (ATTACK TYPE DISTRIBUTION)"
echo "=================================================================="
hdfs dfs -rm -r -f /network/output/attack_dist
hadoop jar "$JAR_PATH" bda.network.AttackDistributionJob /network/raw/network_historical.csv /network/output/attack_dist

echo -e "\n--- MAPREDUCE 3 RESULTS (Attack Category Summary) ---"
hdfs dfs -cat /network/output/attack_dist/part-r-00000

echo -e "\n=================================================================="
echo " STEP 8: RUNNING HDFS REPLICATION & BLOCK SIZE BENCHMARK (PI-2.2.1.1)"
echo "=================================================================="
bash hadoop_mapreduce/scripts/run_replication_test.sh

echo -e "\n=================================================================="
echo " STEP 9: RUNNING JAVA HDFS REPLICATION DEMO API (Replication-Example.doc)"
echo "=================================================================="
java -cp "hadoop_mapreduce/target/classes:$($HADOOP_HOME/bin/hadoop classpath)" bda.network.HDFSReplicationDemo

echo -e "\n=================================================================="
echo " STEP 10: RUNNING WINDOW TRADE-OFF EVALUATOR (PI-3.3.2.1)"
echo "=================================================================="
python3 spark_streaming/window_tradeoff_evaluator.py

echo -e "\n=================================================================="
echo " [SUCCESS] FULL HADOOP BDA PIPELINE EXECUTED ON WSL!"
echo "=================================================================="
echo "HDFS Output Directories:"
hdfs dfs -ls /network/output/
