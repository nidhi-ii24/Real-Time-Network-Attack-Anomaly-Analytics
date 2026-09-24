"""
Dataset Generator for BDA Project
Real-Time Network Attack & Anomaly Analytics using Hadoop, MongoDB and Apache Spark
Course: Big Data Analytics (26ECSC404) - KLE Technological University
"""

import csv
import random
from datetime import datetime, timedelta

PROTOCOLS = ["TCP", "UDP", "ICMP"]
FLAGS = ["SYN", "ACK", "FIN", "RST", "PSH", "URG"]
ATTACK_TYPES = ["BENIGN", "DDoS_FLOOD", "PORT_SCAN", "BRUTE_FORCE", "BOTNET"]

# Target servers inside protected enterprise subnet
SERVERS = ["10.0.0.5", "10.0.0.6", "10.0.0.10", "10.0.0.25"]
COMMON_PORTS = [80, 443, 22, 21, 53, 3306, 8080, 8443]

# Subnets
NORMAL_CLIENT_IPS = [f"192.168.1.{i}" for i in range(10, 80)]
ATTACKER_IPS = [
    "198.51.100.15", "198.51.100.42", "203.0.113.88",
    "203.0.113.199", "185.220.101.5", "185.220.101.99"
]

def generate_record(timestamp, attack_type=None):
    if attack_type is None:
        # 80% benign, 20% attack distribution
        attack_type = random.choices(
            ATTACK_TYPES, weights=[0.80, 0.08, 0.06, 0.04, 0.02], k=1
        )[0]

    ts_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

    if attack_type == "BENIGN":
        src_ip = random.choice(NORMAL_CLIENT_IPS)
        dst_ip = random.choice(SERVERS)
        src_port = random.randint(30000, 65000)
        dst_port = random.choice(COMMON_PORTS)
        protocol = "TCP" if dst_port in [80, 443, 22, 3306, 8080] else random.choice(["TCP", "UDP"])
        packet_count = random.randint(5, 50)
        byte_count = packet_count * random.randint(64, 1500)
        duration = round(random.uniform(0.1, 8.5), 2)
        flag = random.choice(["ACK", "PSH", "FIN"])
        label = "BENIGN"

    elif attack_type == "DDoS_FLOOD":
        src_ip = random.choice(ATTACKER_IPS)
        dst_ip = random.choice(SERVERS[:2]) # Target web servers
        src_port = random.randint(1024, 65535)
        dst_port = random.choice([80, 443])
        protocol = "TCP"
        packet_count = random.randint(800, 4500)
        byte_count = packet_count * random.randint(100, 1400)
        duration = round(random.uniform(0.01, 1.2), 2)
        flag = "SYN"
        label = "DDoS_FLOOD"

    elif attack_type == "PORT_SCAN":
        src_ip = random.choice(ATTACKER_IPS)
        dst_ip = random.choice(SERVERS)
        src_port = random.randint(40000, 60000)
        dst_port = random.randint(1, 1024) # scanning across ports
        protocol = random.choice(["TCP", "UDP"])
        packet_count = random.randint(1, 3)
        byte_count = packet_count * random.randint(40, 80)
        duration = round(random.uniform(0.001, 0.1), 3)
        flag = "SYN" if protocol == "TCP" else "FIN"
        label = "PORT_SCAN"

    elif attack_type == "BRUTE_FORCE":
        src_ip = random.choice(ATTACKER_IPS)
        dst_ip = random.choice(SERVERS)
        src_port = random.randint(35000, 55000)
        dst_port = random.choice([22, 3306, 21]) # SSH / MySQL / FTP
        protocol = "TCP"
        packet_count = random.randint(100, 400)
        byte_count = packet_count * random.randint(80, 300)
        duration = round(random.uniform(1.0, 5.0), 2)
        flag = "PSH"
        label = "BRUTE_FORCE"

    else:  # BOTNET
        src_ip = random.choice(NORMAL_CLIENT_IPS[:5]) # compromised internal host
        dst_ip = "198.51.100.254" # C2 server
        src_port = random.randint(49152, 65535)
        dst_port = 8443
        protocol = "TCP"
        packet_count = random.randint(20, 100)
        byte_count = packet_count * random.randint(200, 800)
        duration = round(random.uniform(0.5, 3.0), 2)
        flag = "ACK"
        label = "BOTNET"

    return [
        ts_str, src_ip, dst_ip, src_port, dst_port,
        protocol, packet_count, byte_count, duration, flag, label
    ]

def generate_historical_dataset(output_file="dataset/network_historical.csv", num_records=25000):
    header = [
        "timestamp", "src_ip", "dst_ip", "src_port", "dst_port",
        "protocol", "packet_count", "byte_count", "duration", "flag", "label"
    ]
    start_time = datetime.now() - timedelta(days=7)
    
    print(f"Generating {num_records} historical network records into '{output_file}'...")
    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for i in range(num_records):
            timestamp = start_time + timedelta(seconds=i * 24)
            writer.writerow(generate_record(timestamp))
            
    print(f"Dataset generation complete! File saved: {output_file}")

if __name__ == "__main__":
    import os
    import sys
    count = 25000
    if len(sys.argv) > 1:
        count = int(sys.argv[1])
    target = os.path.join(os.path.dirname(os.path.abspath(__file__)), "network_historical.csv")
    generate_historical_dataset(target, count)
