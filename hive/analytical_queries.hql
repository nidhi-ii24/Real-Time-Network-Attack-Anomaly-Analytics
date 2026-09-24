-- ==============================================================================
-- HiveQL Analytical Queries for Historical Network Analytics
-- Course: Big Data Analytics (26ECSC404) - KLE Technological University
-- Aligns with PI-5.2.1.1: Visualizations of trends from Hive outputs showing query semantics.
-- ==============================================================================

USE network_analytics_db;

-- Query 1: Top 10 High-Traffic Source IP Addresses ("Top Talkers")
-- Evaluates which hosts generate the highest volume of requests and data transfer
SELECT 
    src_ip,
    COUNT(*) AS total_connections,
    SUM(packet_count) AS total_packets,
    ROUND(SUM(byte_count) / (1024 * 1024), 2) AS total_mbytes
FROM network_events_rcfile
GROUP BY src_ip
ORDER BY total_connections DESC
LIMIT 10;

-- Query 2: Network Protocol Distribution
-- Computes the percentage share of each protocol (TCP, UDP, ICMP)
SELECT 
    protocol,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_share
FROM network_events_rcfile
GROUP BY protocol
ORDER BY event_count DESC;

-- Query 3: Security Event & Attack Breakdown
-- Summarizes benign traffic vs attack categories
SELECT 
    label AS event_category,
    COUNT(*) AS total_occurrences,
    ROUND(AVG(duration), 3) AS avg_duration_sec,
    ROUND(AVG(byte_count), 2) AS avg_bytes_per_flow
FROM network_events_rcfile
GROUP BY label
ORDER BY total_occurrences DESC;

-- Query 4: Hourly Attack Density Trend
-- Aggregates attack occurrences grouped by hour of the day
SELECT 
    SUBSTR(`timestamp`, 1, 13) AS time_hour,
    label,
    COUNT(*) AS occurrences
FROM network_events_rcfile
WHERE label != 'BENIGN'
GROUP BY SUBSTR(`timestamp`, 1, 13), label
ORDER BY time_hour ASC, occurrences DESC;

-- Query 5: Most Targeted Internal Destination Servers & Ports
SELECT 
    dst_ip,
    dst_port,
    COUNT(*) AS attack_probe_count
FROM network_events_rcfile
WHERE label IN ('DDoS_FLOOD', 'PORT_SCAN', 'BRUTE_FORCE')
GROUP BY dst_ip, dst_port
ORDER BY attack_probe_count DESC
LIMIT 10;
