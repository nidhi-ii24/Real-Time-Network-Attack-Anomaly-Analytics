/**
 * MongoDB CRUD Operations & Aggregation Analytics
 * Course: Big Data Analytics (26ECSC404) - KLE Technological University
 * Aligns with Lab Exp 4 & 5, PI-1.4.2.1, and PI-2.2.4
 */

// Switch to network database
db = db.getSiblingDB("network_db");

print("==========================================================");
print(" 1. CREATE (INSERT) OPERATIONS");
print("==========================================================");

// Insert single real-time alert
db.alerts.insertOne({
    alert_id: "ALT-1001",
    timestamp: new Date().toISOString(),
    src_ip: "198.51.100.42",
    dst_ip: "10.0.0.5",
    attack_type: "DDoS_FLOOD",
    metric_count: 850,
    threshold: 100,
    window_type: "TUMBLING_10S",
    severity: "HIGH",
    status: "OPEN",
    mitigation_action: "RATE_LIMIT_APPLIED"
});

// Insert multiple network events
db.network_events.insertMany([
    {
        timestamp: new Date().toISOString(),
        src_ip: "203.0.113.88",
        dst_ip: "10.0.0.10",
        src_port: 48921,
        dst_port: 80,
        protocol: "TCP",
        packet_count: 35,
        byte_count: 2450,
        label: "BENIGN"
    },
    {
        timestamp: new Date().toISOString(),
        src_ip: "185.220.101.5",
        dst_ip: "10.0.0.5",
        src_port: 52310,
        dst_port: 22,
        protocol: "TCP",
        packet_count: 520,
        byte_count: 42000,
        label: "BRUTE_FORCE"
    }
]);

print("[+] Inserted sample alerts and events.");

print("\n==========================================================");
print(" 2. READ (FIND) OPERATIONS");
print("==========================================================");

// Find all HIGH severity alerts
print("\n[A] High Severity Alerts:");
printjson(db.alerts.find({ severity: "HIGH" }).toArray());

// Find events targeting SSH (port 22) with packet_count > 100
print("\n[B] Suspicious SSH Attempts:");
printjson(db.network_events.find({
    dst_port: 22,
    packet_count: { $gt: 100 }
}).toArray());

print("\n==========================================================");
print(" 3. UPDATE OPERATIONS");
print("==========================================================");

// Update status of alert ALT-1001 to RESOLVED
db.alerts.updateOne(
    { alert_id: "ALT-1001" },
    {
        $set: {
            status: "RESOLVED",
            resolved_at: new Date().toISOString(),
            resolution_notes: "Firewall rule created blocking IP 198.51.100.42"
        },
        $inc: { retry_count: 1 }
    }
);
print("[+] Updated alert status to RESOLVED.");

print("\n==========================================================");
print(" 4. AGGREGATION PIPELINES (ANALYTICS)");
print("==========================================================");

// Aggregation 1: Group alerts by attack type with count and avg metric
print("\n[Agg 1] Alert Count and Average Metric by Attack Type:");
printjson(db.alerts.aggregate([
    {
        $group: {
            _id: "$attack_type",
            total_alerts: { $sum: 1 },
            avg_metric_count: { $avg: "$metric_count" },
            max_metric_count: { $max: "$metric_count" }
        }
    },
    { $sort: { total_alerts: -1 } }
]).toArray());

// Aggregation 2: Top Attacking Source IPs
print("\n[Agg 2] Top Attacking IPs by Alert Volume:");
printjson(db.alerts.aggregate([
    {
        $group: {
            _id: "$src_ip",
            alert_count: { $sum: 1 },
            attack_types: { $addToSet: "$attack_type" }
        }
    },
    { $sort: { alert_count: -1 } },
    { $limit: 5 }
]).toArray());

print("\n==========================================================");
print(" 5. DELETE OPERATION (RETENTION PURGE)");
print("==========================================================");

// Delete transient benign events older than current session test
var deleteResult = db.network_events.deleteMany({ label: "TEMP_TEST" });
print("[+] Cleaned up temporary test documents: " + deleteResult.deletedCount);
