-- ==============================================================================
-- Hive Table Definitions: TextFile, RCFile, and ORC
-- Course: Big Data Analytics (26ECSC404) - KLE Technological University
-- Aligns with PI-2.2.4.1: Apply HDFS+Hive RCFile and NoSQL tests for optimal choice.
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS network_analytics_db;
USE network_analytics_db;

-- 1. Raw External TextFile Table (Points directly to HDFS raw CSV)
DROP TABLE IF EXISTS network_events_text;
CREATE EXTERNAL TABLE network_events_text (
    `timestamp` STRING,
    src_ip STRING,
    dst_ip STRING,
    src_port INT,
    dst_port INT,
    protocol STRING,
    packet_count INT,
    byte_count BIGINT,
    duration DOUBLE,
    flag STRING,
    label STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/network/raw'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. RCFile (Record Columnar File) Table
-- Demonstrates hybrid row/column storage format covered in course syllabus
DROP TABLE IF EXISTS network_events_rcfile;
CREATE TABLE network_events_rcfile (
    `timestamp` STRING,
    src_ip STRING,
    dst_ip STRING,
    src_port INT,
    dst_port INT,
    protocol STRING,
    packet_count INT,
    byte_count BIGINT,
    duration DOUBLE,
    flag STRING,
    label STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE;

-- Populate RCFile table from TextFile table
INSERT OVERWRITE TABLE network_events_rcfile
SELECT * FROM network_events_text;

-- 3. Optimized Row Columnar (ORC) Table
-- Demonstrates modern advanced columnar storage with predicate pushdown and zlib compression
DROP TABLE IF EXISTS network_events_orc;
CREATE TABLE network_events_orc (
    `timestamp` STRING,
    src_ip STRING,
    dst_ip STRING,
    src_port INT,
    dst_port INT,
    protocol STRING,
    packet_count INT,
    byte_count BIGINT,
    duration DOUBLE,
    flag STRING,
    label STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="ZLIB", "orc.stripe.size"="67108864");

-- Populate ORC table from TextFile table
INSERT OVERWRITE TABLE network_events_orc
SELECT * FROM network_events_text;

-- Verification
SHOW TABLES;
DESCRIBE FORMATTED network_events_rcfile;
