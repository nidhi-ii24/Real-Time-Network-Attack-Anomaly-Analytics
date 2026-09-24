-- ==============================================================================
-- Benchmark & Trade-off Evaluation: HDFS + Hive RCFile vs NoSQL (MongoDB)
-- Course: Big Data Analytics (26ECSC404) - KLE Technological University
-- Aligns with PI-2.2.4.1: Apply HDFS+Hive RCFile and NoSQL tests for optimal choice.
-- ==============================================================================

USE network_analytics_db;

-- -----------------------------------------------------------------------------
-- Experiment 1: Column Projection Query Benchmark
-- We select only 2 columns out of 11: (src_ip, packet_count)
-- TextFile must read the entire line from disk (row-oriented).
-- RCFile and ORC only read the required column chunks (column-oriented projection).
-- -----------------------------------------------------------------------------

-- Query A: Execute on Raw TextFile
-- In Hive CLI, measure execution time:
-- !time hive -e "SELECT src_ip, SUM(packet_count) FROM network_analytics_db.network_events_text GROUP BY src_ip;"
SELECT src_ip, SUM(packet_count) 
FROM network_events_text 
GROUP BY src_ip;

-- Query B: Execute on RCFile (Record Columnar File)
SELECT src_ip, SUM(packet_count) 
FROM network_events_rcfile 
GROUP BY src_ip;

-- Query C: Execute on ORC (Optimized Row Columnar)
SELECT src_ip, SUM(packet_count) 
FROM network_events_orc 
GROUP BY src_ip;

-- -----------------------------------------------------------------------------
-- Experiment 2: Storage Footprint Inspection
-- Compare file size on HDFS:
-- hdfs dfs -du -h /user/hive/warehouse/network_analytics_db.db/*
-- -----------------------------------------------------------------------------

/*
================================================================================
 EVALUATION & COMPARATIVE MATRIX (FOR REPORT & VIVA - PI-2.2.4.1)
================================================================================

Feature / Dimension          | HDFS + Hive (RCFile/ORC)       | NoSQL (MongoDB Sharded)
-----------------------------|--------------------------------|-----------------------------
Primary Workload Type        | Historical Batch Analytics     | Real-time Event Ingestion & Lookup
Data Model                   | Structured Tabular / Schematized| Dynamic Document (BSON/JSON)
Compression Ratio            | Very High (3x - 5x with ORC/RC)| Moderate (WiredTiger snappy)
Query Latency                | High (seconds to minutes)      | Ultra-Low (< 5 milliseconds)
Sharding / Partitioning      | Hive Partitions + HDFS Blocks  | Range / Hashed Sharding
Replication Mechanism        | HDFS Block Replication         | Replica Set Oplog Sync
Optimal Use Case in Project  | Historical security trends,    | Live streaming anomaly alerts,
                             | massive multi-day log auditing | instant dashboard notification
================================================================================
Conclusion:
A hybrid Lambda Architecture is optimal:
- HDFS + Hive RCFile serves the batch layer for massive historical query throughput.
- MongoDB serves the speed/serving layer for low-latency alert dissemination.
================================================================================
*/
