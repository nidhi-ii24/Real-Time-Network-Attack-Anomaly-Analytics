# Complete Step-by-Step Execution Guide
## Real-Time Network Attack & Anomaly Analytics using Hadoop, MongoDB and Apache Spark
**Course:** Big Data Analytics (26ECSC404) — KLE Technological University

---

This guide contains the complete execution procedure divided into two tracks:
1. **Track 1: Immediate End-to-End Demo (Windows Native)** — Ready to run right now on your machine with 0 additional setup.
2. **Track 2: Full Big Data Cluster Execution (Ubuntu / WSL2 / College Lab)** — Commands for HDFS, Java MapReduce, Hive RCFile, and MongoDB Sharding/Replication.
3. **Track 3: 5-Minute Examiner Presentation Walkthrough** — Exactly what to click, run, and say during your project evaluation.

---

# 🚀 Track 1: Immediate End-to-End Demo (Windows Native)

You already have Python, Streamlit, PyMongo, and MongoDB running on your laptop. You can execute this complete live pipeline immediately in separate terminal tabs.

### Step 1: Verify / Generate Historical Dataset
Open PowerShell in `d:\7th sem\BDA\BDA Project`:
```powershell
python dataset/generate_dataset.py 25000
```
**Expected Output:**
```text
Generating 25000 historical network records into '.../network_historical.csv'...
Dataset generation complete! File saved: .../dataset/network_historical.csv
```

---

### Step 2: Launch the Streamlit Security Dashboard
In PowerShell (Terminal 1):
```powershell
python -m streamlit run dashboard/app.py
```
**Action:**
- The dashboard automatically opens in your browser at `http://localhost:8501`.
- It will connect to your local MongoDB at `mongodb://localhost:27017/`.
- Keep this terminal running in the background.

---

### Step 3: Run the Window Trade-off Evaluator (PI-3.3.2.1)
In PowerShell (Terminal 2):
```powershell
python spark_streaming/window_tradeoff_evaluator.py
```
**Expected Output:**
```text
==================================================================
 WINDOW TRADE-OFF BENCHMARK: TUMBLING VS SLIDING WINDOWS (PI-3.3.2.1)
==================================================================
[*] Simulating 2000 events with injected ground-truth anomalies...

------------------------------------------------------------------
Metric                         | Tumbling Window (10s)  | Sliding Window (30s/5s)
------------------------------------------------------------------
Window Type                    | Fixed, Non-overlapping | Continuous, Overlapping
State Memory Overhead          | Low (cleared per window) | Moderate (buffers 30s)
Mean Detection Latency         | 13.23 s                | 20.26 s               
Fast Volumetric Flood Recall   | 98.5% (High)           | 99.2% (High)          
Stealth / Distributed Recall   | 62.0% (Straddle loss)  | 96.8% (Captures scan) 
Computational Cost             | O(N) single-pass       | O(N * W/S) multi-eval 
------------------------------------------------------------------
```
*Note: This directly proves the Course Performance Indicator 3.3.2.1.*

---

### Step 4: Start the Live Network Event Stream Simulator
In PowerShell (Terminal 3):
```powershell
python dataset/live_stream_simulator.py --port 9999 --rate 10 --inject-interval 20
```
**Expected Output:**
```text
[*] Network Event Streamer listening on 0.0.0.0:9999
[*] Connect Apache Spark Streaming or netcat: nc localhost 9999
```
*Leave this running. It acts as the live network traffic source.*

---

### Step 5: Start the Spark Streaming Anomaly Detection Engine
In PowerShell (Terminal 4):
```powershell
python spark_streaming/spark_streaming_anomaly_detector.py
```
**Expected Output:**
```text
==================================================================
 APACHE SPARK / STREAMING ANOMALY DETECTION ENGINE
 Connecting to Event Stream at 127.0.0.1:9999
 Tumbling Window: 10s (Flood Detection)
 Sliding Window: 30s Window / 5s Slide (Port Scan & Brute Force)
 Output Sink: MongoDB (network_db.alerts)
==================================================================
[+] Connected to network event generator on 127.0.0.1:9999!
[+] Successfully connected to MongoDB at mongodb://localhost:27017/
```
Within 20 seconds, you will see real-time detection logs:
```text
[ALERT - TUMBLING WINDOW] DDoS_FLOOD from 198.51.100.42 (Count: 150)
[ALERT - SLIDING WINDOW] PORT_SCAN from 203.0.113.88 (Probed: 45 ports)
```
Switch over to the Streamlit dashboard in your browser and watch the alert counter and tables update live!

---

### Step 6: Test On-Demand Anomaly Injection via Dashboard
1. On the Streamlit dashboard (`http://localhost:8501`), click on the **"⚡ Interactive Attack Simulator"** tab in the sidebar.
2. Click **"🚀 Launch DDoS Attack Burst"**.
3. Go to the **"🚨 Live Real-Time Alerts"** tab:
   - Notice the **DDoS Floods Detected** metric increments immediately.
   - The alert table shows the red badge `🔴 HIGH` with source IP `198.51.100.42`.
4. Click **"🚀 Launch Port Scan Probe"**:
   - The **Port Scans Caught** metric increments via the 30-second sliding window.

---

# 🐧 Track 2: Full Big Data Cluster Execution (Ubuntu / WSL2 / Lab)

Follow these steps when demonstrating on Ubuntu in WSL2, VMware, or your university lab machine.

### Phase 1: Environment Setup in Ubuntu
```bash
# 1. Update system & install OpenJDK 11
sudo apt update && sudo apt install -y openjdk-11-jdk ssh pdsh maven

# 2. Configure SSH localhost access (Required for Hadoop daemons)
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost   # Verify: type 'yes' and then 'exit'

# 3. Download & Extract Hadoop 3.3.6 (if not already installed)
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop

# 4. Add to ~/.bashrc:
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export HADOOP_HOME=/usr/local/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc
```

---

### Phase 2: Start Hadoop Daemons
Format the NameNode (only once on initial setup):
```bash
hdfs namenode -format
```

Start HDFS and YARN daemons:
```bash
start-dfs.sh
start-yarn.sh
```

Verify with `jps`:
```bash
jps
```
**Expected Output:**
```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
Jps
```

---

### Phase 3: HDFS Data Ingestion & Replication Benchmarking (PI-2.2.1.1)

Navigate to the project directory:
```bash
cd "d:/7th sem/BDA/BDA Project"   # Or /mnt/d/7th\ sem/BDA/BDA\ Project in WSL
```

#### 1. Setup HDFS Folders & Upload Dataset
```bash
bash hadoop_mapreduce/scripts/run_hdfs_setup.sh
```

#### 2. Run the HDFS Replication & Block Size Benchmark
```bash
bash hadoop_mapreduce/scripts/run_replication_test.sh
```
**What this demonstrates to the examiner:**
- Compares throughput between 1MB block size and 64MB block size.
- Changes replication factor dynamically from 1 to 2 using `hdfs dfs -setrep 2 ...`.
- Shows `hdfs fsck` reporting under-replicated status due to single-node DataNode.

#### 3. Run the Java HDFS Replication Demo API
```bash
# Compile and run the Java API demo (matching Replication-Example.doc)
cd hadoop_mapreduce
javac -cp $(hadoop classpath) src/main/java/bda/network/HDFSReplicationDemo.java -d target/classes
java -cp target/classes:$(hadoop classpath) bda.network.HDFSReplicationDemo
cd ..
```

---

### Phase 4: Compile & Run Hadoop MapReduce Jobs (Lab Exp 3)

#### 1. Build the MapReduce JAR
```bash
cd hadoop_mapreduce
mvn clean package -DskipTests
cd ..
```
*This generates `hadoop_mapreduce/target/network-analytics-mapreduce-1.0-SNAPSHOT.jar`.*

#### 2. Execute MapReduce Job 1: Protocol Distribution
```bash
# Remove old output if exists
hdfs dfs -rm -r /network/output/protocol_counts

# Run MapReduce Job
hadoop jar hadoop_mapreduce/target/network-analytics-mapreduce-1.0-SNAPSHOT.jar \
    bda.network.ProtocolCountJob \
    /network/raw/network_historical.csv \
    /network/output/protocol_counts

# View Output
hdfs dfs -cat /network/output/protocol_counts/part-r-00000
```
**Expected Output:**
```text
ICMP    1240
TCP     18920
UDP     4840
```

#### 3. Execute MapReduce Job 2: Top Source IPs ("Top Talkers")
```bash
hdfs dfs -rm -r /network/output/top_ips

hadoop jar hadoop_mapreduce/target/network-analytics-mapreduce-1.0-SNAPSHOT.jar \
    bda.network.TopSourceIPJob \
    /network/raw/network_historical.csv \
    /network/output/top_ips

hdfs dfs -cat /network/output/top_ips/part-r-00000 | sort -k2 -nr | head -n 10
```

#### 4. Execute MapReduce Job 3: Attack Category Breakdown
```bash
hdfs dfs -rm -r /network/output/attack_dist

hadoop jar hadoop_mapreduce/target/network-analytics-mapreduce-1.0-SNAPSHOT.jar \
    bda.network.AttackDistributionJob \
    /network/raw/network_historical.csv \
    /network/output/attack_dist

hdfs dfs -cat /network/output/attack_dist/part-r-00000
```
**Expected Output:**
```text
BENIGN          20015
BOTNET          504
BRUTE_FORCE     1002
DDoS_FLOOD      2014
PORT_SCAN       1465
```

---

### Phase 5: Apache Hive Columnar Analytics (PI-2.2.4.1 & Lab Exp 6, 7)

Open Hive CLI:
```bash
hive
```

Execute Table Creation (TextFile, RCFile, ORC):
```sql
SOURCE hive/create_tables.hql;
```

Execute Analytical Queries:
```sql
SOURCE hive/analytical_queries.hql;
```

Run RCFile vs TextFile Performance Benchmark:
```sql
SOURCE hive/rcfile_vs_text_benchmark.hql;
```
**Key Viva Observation:** RCFile/ORC projection queries skip reading unrequested column bytes, reducing I/O time by over 60% compared to TextFile.

---

### Phase 6: MongoDB Replication, Sharding & CRUD (PI-1.4.2.1 & Lab Exp 4, 5)

Connect to MongoDB shell:
```bash
mongosh
```

#### 1. Execute Replica Set Configuration
```javascript
load("mongodb/setup_replica_set.js");
```
*Verifies: 3-member replica set `rs0` with primary election and `{ w: "majority" }` write concern.*

#### 2. Execute Sharding Setup
```javascript
load("mongodb/setup_sharding.js");
```
*Verifies: Sharded collection `network_events` with hashed key on `src_ip`.*

#### 3. Execute CRUD & Aggregation Analytics
```javascript
load("mongodb/crud_operations.js");
```
*Verifies: Complete Create, Read, Update, Delete, and 4 advanced aggregation pipelines.*

---

# 🎓 Track 3: 5-Minute Examiner Presentation Walkthrough

Follow this exact script when presenting to your professor or external evaluator:

1. **Slide / Introduction (30 seconds)**:
   > *"Good morning. For our Big Data Analytics project, we designed and implemented a unified Lambda Architecture: **Real-Time Network Attack & Anomaly Analytics using Hadoop, MongoDB and Apache Spark**. We separate historical batch analytics on HDFS and Hive from real-time stream processing on Spark and MongoDB."*

2. **HDFS & MapReduce Demonstration (1 minute)**:
   - Show terminal with `jps` displaying NameNode, DataNode, ResourceManager.
   - Show `run_replication_test.sh` demonstrating `dfs.replication` and block size effects on throughput (satisfying PI-2.2.1.1).
   - Display the output of MapReduce Jobs (`hdfs dfs -cat /network/output/protocol_counts/part-r-00000`).

3. **Apache Hive & RCFile vs NoSQL Comparison (1 minute)**:
   - Open `rcfile_vs_text_benchmark.hql` and show how RCFile and ORC optimize columnar projection over raw TextFile (satisfying PI-2.2.4.1).
   - Explain why MongoDB is selected for the speed layer while Hive RCFile serves the batch layer.

4. **Window Trade-Off Evaluation (1 minute)**:
   - Run `python spark_streaming/window_tradeoff_evaluator.py`.
   - Explain PI-3.3.2.1:
     > *"We evaluated Tumbling (10s) vs Sliding (30s/5s) windows. Tumbling windows provide rapid sub-15s response with zero state memory for high-volume DDoS floods, while Sliding windows eliminate window-boundary straddling to achieve 96.8% recall for stealth port scans."*

5. **Live Interactive Dashboard Demo (1.5 minutes)**:
   - Bring up the Streamlit dashboard on screen (`http://localhost:8501`).
   - Navigate to **"⚡ Interactive Attack Simulator"**.
   - Click **"Launch DDoS Attack Burst"** and **"Launch Port Scan Probe"**.
   - Navigate to **"🚨 Live Real-Time Alerts"** and show the examiner the alerts being written directly into MongoDB and displayed on the UI with severity ratings.
   - Conclude by showing the **"📊 Historical Analytics"** tab displaying the visualized query semantics from Hive and MapReduce.
