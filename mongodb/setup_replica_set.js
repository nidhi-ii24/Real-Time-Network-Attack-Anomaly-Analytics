/**
 * MongoDB Replica Set Configuration Script
 * Course: Big Data Analytics (26ECSC404) - KLE Technological University
 * Aligns with PI-1.4.2.1: Apply NoSQL schema with sharding/replication.
 *
 * Architecture:
 * 1 Primary (port 27017) + 2 Secondaries (port 27018, 27019)
 * Provides automatic failover, high availability, and read preference distribution.
 */

// Step 1: Initiate the Replica Set on the Primary Node
rs.initiate({
    _id: "rs0",
    members: [
        { _id: 0, host: "localhost:27017", priority: 2 }, // Preferred Primary
        { _id: 1, host: "localhost:27018", priority: 1 }, // Secondary
        { _id: 2, host: "localhost:27019", priority: 1 }  // Secondary
    ]
});

// Step 2: Verify Replica Set Status & Roles
sleep(2000);
print("\n--- REPLICA SET STATUS ---");
printjson(rs.status());

// Step 3: Verify Master / Replica Configuration
print("\n--- REPLICA SET CONFIGURATION ---");
printjson(rs.conf());

// Step 4: Test Write Concern with Majority Acknowledgment
db = db.getSiblingDB("network_db");
db.network_events.insertOne(
    {
        timestamp: new Date().toISOString(),
        src_ip: "198.51.100.42",
        dst_ip: "10.0.0.5",
        protocol: "TCP",
        event_type: "REPLICA_VERIFICATION_TEST",
        severity: "INFO"
    },
    { writeConcern: { w: "majority", wtimeout: 5000 } }
);

print("\n[+] Verification document successfully replicated to majority nodes.");
