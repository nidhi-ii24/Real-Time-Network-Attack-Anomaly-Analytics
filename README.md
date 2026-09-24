# Real-Time Network Attack & Anomaly Analytics using Hadoop, MongoDB and Apache Spark

[![Course](https://img.shields.io/badge/Course-BDA%20(26ECSC404)-blue.svg)](file:///docs/PROJECT_REPORT.md)
[![Institution](https://img.shields.io/badge/Institution-KLE%20Technological%20University-orange.svg)](file:///docs/PROJECT_REPORT.md)
[![Status](https://img.shields.io/badge/Status-Complete%20Production%20Harness-brightgreen.svg)](file:///README.md)

An end-to-end Big Data Analytics project combining distributed batch processing (**Hadoop HDFS, MapReduce, Hive RCFile**) with real-time stream processing (**Apache Spark Structured Streaming, Tumbling/Sliding Windowing**) and distributed NoSQL storage (**MongoDB Replica Set & Sharding**) visualized via an interactive security dashboard (**Streamlit**).

---

## 🏛️ System Architecture

```text
                                NETWORK EVENT SOURCES
                                          │
                     ┌────────────────────┴────────────────────┐
                     │                                         │
           Historical Dataset (CICIDS2017)           Live Event Stream Generator
                     │                                (TCP Socket Port 9999)
                     ↓                                         │
              HADOOP HDFS                                      ↓
      (dfs.replication, 64MB Blocks)                 APACHE SPARK STREAMING
         ┌───────────┴───────────┐                             │
         ↓                       ↓                   ┌─────────┴─────────┐
     MapReduce              Apache Hive              ↓                   ↓
  (Java Analytics)        (RCFile vs ORC)     Tumbling Window     Sliding Window
         │                       │              (10s Floods)       (30s Scans)
         └───────────┬───────────┘                   │                   │
                     │                               └─────────┬─────────┘
                     │                                         ↓
                     │                              Anomaly Detection Engine
                     │                                         │
                     │                                         ↓
                     │                                      MONGODB
                     │                              (Replica Set + Sharded)
                     │                                         │
                     └───────────────────┬─────────────────────┘
                                         ↓
                            STREAMLIT SECURITY DASHBOARD
                                         │
                             ┌───────────┴───────────┐
                             ↓                       ↓
                     Historical Trends          Live Alerts
```

---

## 🎯 Alignment with KLE Tech Course Rubrics & PIs

| Evaluation Phase | Course PI | Implementation / Verification Deliverable |
| :--- | :--- | :--- |
| **Problem Identification** | **PI-2.2.1** | Scope covers volumetric DDoS floods, stealth port scans, brute force, and multi-year historical log auditing. |
| **Data Preparation** | **PI-1.4.2** | Realistic 11-dimension CICIDS2017 flow generator (`generate_dataset.py`) & socket replay simulator (`live_stream_simulator.py`). |
| **Storage Architecture** | **PI-2.2.1.1** | HDFS replication test script (`run_replication_test.sh`) & Java API (`HDFSReplicationDemo.java`) evaluating block sizes and `setReplication()`. |
| **NoSQL Storage & Scalability** | **PI-1.4.2.1** | MongoDB 3-node Replica Set (`setup_replica_set.js`) & Sharding on `{ src_ip: 'hashed' }` (`setup_sharding.js`) with CRUD operations. |
| **Real-Time Stream Processing** | **PI-3.2.2.1** | Spark Streaming pipeline ingesting socket flows, applying stateful transformations, and emitting alerts to MongoDB. |
| **Window Trade-Off Evaluation** | **PI-3.3.2.1** | Empirical evaluator (`window_tradeoff_evaluator.py`) measuring latency vs accuracy for Tumbling vs Sliding windows. |
| **High-Performance Querying** | **PI-2.2.4.1** | Hive table schemas (`create_tables.hql`) comparing TextFile vs RCFile vs ORC compression and query speeds against NoSQL. |
| **Data Visualization & Semantics** | **PI-5.2.1.1** | Multi-tab Streamlit dashboard (`dashboard/app.py`) displaying live alert streams, window metrics, and historical query semantics. |
| **Report & Presentation** | **PI-9.1.2** | Academic report (`docs/PROJECT_REPORT.md`) and comprehensive 14-question viva guide (`docs/VIVA_PREPARATION.md`). |

---

## 📂 Repository Structure

```text
d:\7th sem\BDA\BDA Project\
├── README.md                                  # Complete Project Overview & Run Guide
├── docs\
│   ├── PROJECT_REPORT.md                      # Academic report mapped to course rubrics
│   └── VIVA_PREPARATION.md                    # In-depth viva Q&A for laboratory & project exams
├── dataset\
│   ├── generate_dataset.py                    # Generates historical dataset (CICIDS2017 schema)
│   ├── network_historical.csv                 # 15,000+ historical records
│   └── live_stream_simulator.py               # Live socket event generator with anomaly injection
├── hadoop_mapreduce\
│   ├── pom.xml                                # Maven build configuration
│   ├── src\main\java\bda\network\
│   │   ├── ProtocolCountJob.java              # Job 1: Protocol distribution (TCP/UDP/ICMP)
│   │   ├── TopSourceIPJob.java                # Job 2: High volume source IPs ("Top Talkers")
│   │   ├── AttackDistributionJob.java         # Job 3: Attack category distribution
│   │   └── HDFSReplicationDemo.java           # Java API demo of dfs.replication & setReplication()
│   └── scripts\
│       ├── run_hdfs_setup.sh                  # HDFS directory creation and dataset upload
│       └── run_replication_test.sh            # Benchmark for block sizes & replication throughput
├── mongodb\
│   ├── setup_replica_set.js                   # 3-Node replica set setup script
│   ├── setup_sharding.js                      # Config server, mongos, and hashed sharding setup
│   ├── crud_operations.js                     # CRUD and aggregation queries for lab certification
│   └── mongo_helper.py                        # Python client with automated fallback store
├── spark_streaming\
│   ├── spark_streaming_anomaly_detector.py    # PySpark streaming engine (Tumbling & Sliding)
│   └── window_tradeoff_evaluator.py           # Evaluates latency vs accuracy trade-offs
├── hive\
│   ├── create_tables.hql                      # TextFile, RCFile, and ORC schema definitions
│   ├── analytical_queries.hql                 # HiveQL analytical queries
│   └── rcfile_vs_text_benchmark.hql           # Benchmark script comparing RCFile vs NoSQL
└── dashboard\
    └── app.py                                 # Streamlit Real-Time & Historical Dashboard
```

---

## 🚀 Quickstart & Execution Guide

### 1. Generate Historical Dataset
```bash
python dataset/generate_dataset.py 25000
```

### 2. Run the Window Trade-off Evaluator (PI-3.3.2.1)
Empirically compare Tumbling vs. Sliding window performance:
```bash
python spark_streaming/window_tradeoff_evaluator.py
```

### 3. Launch the Interactive Dashboard
```bash
streamlit run dashboard/app.py
```
Open [http://localhost:8501](http://localhost:8501) in your browser.

### 4. Run the Real-Time Streaming Pipeline
In Terminal 1 (Start the live event stream):
```bash
python dataset/live_stream_simulator.py --port 9999 --rate 10
```

In Terminal 2 (Start the Spark Anomaly Detection Engine):
```bash
python spark_streaming/spark_streaming_anomaly_detector.py
```

### 5. Running Hadoop MapReduce on HDFS (in Ubuntu WSL / Cluster)
```bash
# Upload data to HDFS
bash hadoop_mapreduce/scripts/run_hdfs_setup.sh

# Run Protocol Count Job
hadoop jar target/network-analytics-mapreduce-1.0-SNAPSHOT.jar bda.network.ProtocolCountJob /network/raw /network/output/protocol_counts

# Run Top Source IP Job
hadoop jar target/network-analytics-mapreduce-1.0-SNAPSHOT.jar bda.network.TopSourceIPJob /network/raw /network/output/top_ips

# Run Replication Benchmark
bash hadoop_mapreduce/scripts/run_replication_test.sh
```

### 6. Executing Hive Analytical Queries (in Hive CLI)
```bash
hive -f hive/create_tables.hql
hive -f hive/analytical_queries.hql
```

### 7. Configuring MongoDB Replica Set & Sharding (in mongosh)
```bash
mongosh --port 27017 < mongodb/setup_replica_set.js
mongosh --port 27017 < mongodb/setup_sharding.js
mongosh --port 27017 < mongodb/crud_operations.js
```
"# Real-Time-Network-Attack-Anomaly-Analytics" 
