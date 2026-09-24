/**
 * MongoDB Sharded Cluster Configuration Script
 * Course: Big Data Analytics (26ECSC404) - KLE Technological University
 * Aligns with PI-1.4.2.1: Apply NoSQL schema with sharding/replication.
 *
 * Architecture:
 * - Mongos Router: localhost:27017
 * - Config Server Replica Set: localhost:27019
 * - Shard 1 (Replica Set / Standalone): localhost:27018
 * - Shard 2 (Replica Set / Standalone): localhost:27020
 */

// Step 1: Add Shards to Cluster via mongos
sh.addShard("shard1/localhost:27018");
sh.addShard("shard2/localhost:27020");

print("\n--- SHARD STATUS ---");
printjson(sh.status());

// Step 2: Enable Sharding on Database
sh.enableSharding("network_db");
print("[+] Sharding enabled on database: network_db");

// Step 3: Shard Collections with Appropriate Shard Keys
// A. network_events: Sharded using HASHED key on src_ip to achieve even distribution
//    across all shards and prevent hotspotting during volumetric DDoS attacks.
sh.shardCollection("network_db.network_events", { "src_ip": "hashed" });
print("[+] network_db.network_events sharded on { src_ip: 'hashed' }");

// B. alerts: Sharded using COMPOUND / RANGE key { timestamp: 1, severity: 1 }
//    enabling efficient time-range analytical scans.
sh.shardCollection("network_db.alerts", { "timestamp": 1 });
print("[+] network_db.alerts sharded on { timestamp: 1 }");

// Step 4: Verify Sharding Distribution
print("\n--- DISTRIBUTION DETAILS ---");
db = db.getSiblingDB("network_db");
printjson(db.network_events.getShardDistribution());
