# Comprehensive Viva & Technical Interview Guide
## Real-Time Network Attack & Anomaly Analytics using Hadoop, MongoDB and Apache Spark
**Course:** Big Data Analytics (26ECSC404) — KLE Technological University

---

## Part 1: HDFS & Distributed Storage (CO2, PI-2.2.1.1)

### Q1. What is HDFS, and why is it preferred over traditional file systems for Big Data?
**Answer:**  
HDFS (Hadoop Distributed File System) is a distributed, user-space file system designed to run on commodity hardware. Unlike traditional file systems (like NTFS or ext4) which reside on a single machine with small 4KB blocks, HDFS splits large files into large blocks (typically 64MB or 128MB) and distributes them across multiple DataNodes.  
Key advantages:
1. **High Fault Tolerance**: Built-in block replication ensures data remains available even when physical DataNodes fail.
2. **Streaming Data Access (Write Once, Read Many)**: Optimized for sequential batch throughput rather than random write access.
3. **Data Locality**: Computation is moved to the data (via MapReduce/Spark) rather than moving multi-gigabyte data across the network to compute nodes.

---

### Q2. Explain the difference between HDFS block splitting and MongoDB sharding.
**Answer (Crucial Distinction):**  
- **HDFS Block Distribution**: Operates at the **raw physical file level**. A file is chopped blindly into fixed-size chunks (e.g., 64MB or 128MB) without understanding record boundaries or schemas. The NameNode maps which physical DataNodes store each block.
- **MongoDB Sharding**: Operates at the **logical database / document level**. Records are partitioned into chunks based on a specific **Shard Key** (e.g., `{ src_ip: "hashed" }` or `{ timestamp: 1 }`). MongoDB inspects document fields, routes queries via the `mongos` router, and dynamically balances chunks across shard nodes.

---

### Q3. What is the role of NameNode, DataNode, and Secondary NameNode?
**Answer:**  
- **NameNode (Master)**: Manages file system namespace, directory tree, file-to-block mappings, and DataNode health (via periodic heartbeats every 3 seconds). It stores metadata in memory (RAM) and persists it via `fsimage` and `edits` log.
- **DataNode (Worker)**: Stores actual data blocks on local disk storage, serves read/write requests from clients, and sends periodic Block Reports to the NameNode.
- **Secondary NameNode**: **Not a standby NameNode!** It performs a periodic *checkpointing* process: merging the active `edits` log with the baseline `fsimage` so that the NameNode's restart recovery time remains short.

---

### Q4. How does `dfs.replication` and `fs.setReplication()` work? What happens on a single-node cluster when replication is set to 2?
**Answer:**  
- `dfs.replication` defines the default replication factor configured in `hdfs-site.xml` or set programmatically via `conf.set("dfs.replication", "1")` prior to file creation.
- `fs.setReplication(Path path, short replication)` dynamically alters the target replication factor of an already-existing file in the NameNode metadata.
- **On a Single-Node Pseudo-Distributed Cluster**: When you execute `setReplication(path, (short) 2)` or `hdfs dfs -setrep -w 2`, the NameNode updates its metadata target to 2. However, because HDFS enforces the rack-awareness rule that two copies of the same block *cannot* reside on the same physical DataNode daemon, the block is flagged as **Under-Replicated** until a second DataNode joins the cluster.

---

### Q5. What is the effect of block size on cluster performance (PI-2.2.1.1)?
**Answer:**  
- **Small Block Size (e.g., 1MB)**: Generates a huge number of blocks. Because each block metadata entry occupies ~150 bytes in NameNode RAM, millions of small blocks exhaust NameNode memory and cause excessive disk seek latency during MapReduce processing.
- **Optimal Block Size (64MB - 128MB)**: Amortizes disk seek time, enabling streaming sequential reads at disk transfer rate (> 100 MB/s), maximizes data locality, and allows MapReduce Mappers to process substantial chunks in parallel.

---

## Part 2: Hadoop MapReduce (CO1, CO3, PI-2.2.4)

### Q6. Walk through the complete lifecycle of a MapReduce job.
**Answer:**  
1. **InputFormat & InputSplit**: Input files in HDFS are split logically into `InputSplit` instances (typically 1 split per HDFS block).
2. **RecordReader**: Translates each split into `<Key, Value>` pairs (e.g., `LongWritable` line offset as Key, `Text` line content as Value).
3. **Mapper (`map()` method)**: Processes each record and emits intermediate `<Key, Value>` pairs (e.g., `(TCP, 1)`).
4. **Combiner (Optional)**: Mini-reducer executed locally on the Mapper node to pre-aggregate intermediate output (e.g., sum TCP locally) and reduce network bandwidth consumption during shuffle.
5. **Partitioner**: Determines which Reducer receives each intermediate key using hash partitioning: `hash(key) % numReducers`.
6. **Shuffle & Sort**: Framework transfers partitioned data across the network to destination Reducers and sorts records by key.
7. **Reducer (`reduce()` method)**: Iterates over all values associated with each unique key and emits the final consolidated output.
8. **OutputFormat**: Writes final results into HDFS files (e.g., `part-r-00000`).

---

### Q7. What MapReduce jobs did you implement for this network security project?
**Answer:**  
1. **`ProtocolCountJob`**: Extracts transport protocols (token 5) and aggregates total occurrences of TCP, UDP, and ICMP.
2. **`TopSourceIPJob`**: Extracts `src_ip` (token 1) and counts flow occurrences per IP to detect volumetric traffic sources ("Top Talkers").
3. **`AttackDistributionJob`**: Extracts security classification `label` (token 10) and summarizes the proportions of BENIGN traffic versus attack categories (`DDoS_FLOOD`, `PORT_SCAN`, `BRUTE_FORCE`, `BOTNET`).

---

## Part 3: Apache Spark & Stream Processing (CO3, CO4, PI-3.2.2.1, PI-3.3.2.1)

### Q8. What is the fundamental architectural difference between Hadoop MapReduce and Apache Spark?
**Answer:**  
- **MapReduce**: Primarily disk-bound. Intermediate shuffle data is spilled to local disk, and subsequent stages read from disk. Highly reliable for overnight batch processing, but incurs substantial I/O latency.
- **Apache Spark**: Primarily memory-centric. Data is abstracted as Resilient Distributed Datasets (RDDs) or DataFrames cached in executor RAM. Operates using Directed Acyclic Graph (DAG) query planning with pipelined in-memory transformations, executing jobs 10x to 100x faster than MapReduce.

---

### Q9. Compare Tumbling Window vs. Sliding Window (PI-3.3.2.1). What are their accuracy-latency trade-offs?
**Answer:**  
- **Tumbling Window**:
  - Non-overlapping, contiguous time buckets (e.g., 10s: `[0-10s]`, `[10-20s]`).
  - *Latency*: Very fast. Emits alerts immediately at the window boundary.
  - *Memory/State*: Extremely low. State is discarded immediately after window evaluation.
  - *Trade-off / Weakness*: **Boundary Straddling**. If an attacker probes 10 ports between 00:08 and 00:12, a 10s tumbling window splits this into 5 ports in window 1 and 5 ports in window 2. Neither exceeds the threshold (e.g., 8), resulting in a **False Negative**.
- **Sliding Window**:
  - Overlapping time windows (e.g., 30s window with 5s slide step: `[0-30s]`, `[5-35s]`).
  - *Latency*: Emits evaluation every slide step (5s).
  - *Memory/State*: Higher. Requires retaining event history over the entire 30s duration in state memory.
  - *Trade-off / Strength*: High detection accuracy (96.8% recall). Seamlessly catches distributed, slow, or boundary-crossing stealth attacks.

---

### Q10. What are stateless vs. stateful stream operations in your pipeline?
**Answer:**  
- **Stateless Operations**: Transformations evaluated strictly on the individual incoming record without context of previous records (e.g., filtering IP format, parsing JSON payload, extracting ports).
- **Stateful Operations**: Transformations that track historical state across time (e.g., tumbling/sliding window aggregations, maintaining unique probed port sets per source IP).

---

## Part 4: NoSQL & MongoDB (CO2, PI-1.4.2.1)

### Q11. Explain MongoDB Replica Sets and the write concern `w: "majority"`.
**Answer:**  
- A MongoDB Replica Set is an ensemble of `mongod` nodes maintaining identical data copies. One node is elected **Primary** (receives all client writes), while other nodes act as **Secondaries** (replicate changes asynchronously via the `oplog`).
- If the Primary crashes, an automated Raft-like election algorithm promotes a Secondary with the most up-to-date oplog to become the new Primary within seconds.
- **Write Concern `{ w: "majority" }`**: Requires that a write is acknowledged only after being committed to disk on a majority of replica members (e.g., 2 out of 3 nodes), guaranteeing zero data loss during primary failover.

---

### Q12. Why did you choose `{ src_ip: "hashed" }` as the shard key for `network_events`?
**Answer:**  
In high-volume network security scenarios, volumetric DDoS floods generate hundreds of thousands of events from a concentrated set of IP addresses.  
- If a **Range-based** shard key were used on `timestamp`, all current writes would target a single shard server (hotspotting the active time range).
- By utilizing a **Hashed Shard Key** on `src_ip`, the MD5 hashes of IP addresses are uniformly distributed across the cluster shards, ensuring balanced I/O distribution and horizontal write scaling.

---

## Part 5: Apache Hive & Columnar Formats (CO5, PI-2.2.4.1, PI-5.2.1.1)

### Q13. Compare Hive TextFile, RCFile, and ORC file formats.
**Answer:**  
- **TextFile**: Plain CSV/TSV format. Row-oriented. High disk consumption, zero compression, and Hive must read the entire line from disk even if only 1 column is requested.
- **RCFile (Record Columnar File)**: Hybrid row/column format. Partitions data into row groups (typically 4MB), and inside each row group, stores column values contiguously. Enables column projection pushdown and column-level compression.
- **ORC (Optimized Row Columnar)**: Advanced columnar format. Groups data into Stripes (typically 64MB - 256MB), includes built-in column statistics (Min, Max, Sum, Count), bloom filters, and dictionary encoding. Enables predicate pushdown: Hive skips entire stripes without reading from disk if stripe statistics show no matching rows.

---

### Q14. Why is a Hybrid Architecture (HDFS + Hive for Batch, MongoDB for Speed) superior to using either alone?
**Answer (Directly addresses PI-2.2.4.1):**  
- **HDFS + Hive** is unbeatable for bulk sequential scans over months of historical logs (terabytes to petabytes) with optimal compression, but is too slow for sub-second real-time alert queries.
- **MongoDB** is unbeatable for sub-second point lookups, dynamic schema JSON alerts, and live dashboard rendering, but is inefficient and expensive for multi-terabyte full-table analytical aggregates.
- Combining both in a **Lambda Architecture** leverages the unique strengths of each system.
