"""
Live Network Event Stream Simulator
BDA Project: Real-Time Network Attack & Anomaly Analytics
Course: Big Data Analytics (26ECSC404) - KLE Technological University

Supports:
1. TCP Socket streaming on localhost:9999 (for Apache Spark Streaming `readStream.format('socket')`)
2. Optional direct ingestion to MongoDB `network_db.network_events`
3. Controlled anomaly injection:
   - 'ddos': Volumetric traffic flood from a single IP
   - 'portscan': Rapid port probe across hundreds of ports
   - 'bruteforce': Repeated authorization bursts against SSH/MySQL
"""

import socket
import time
import json
import random
import argparse
from datetime import datetime

# Server configurations
HOST = "0.0.0.0"
PORT = 9999

SERVERS = ["10.0.0.5", "10.0.0.6", "10.0.0.10"]
COMMON_PORTS = [80, 443, 22, 21, 53, 3306, 8080]
NORMAL_IPS = [f"192.168.1.{i}" for i in range(10, 50)]

def make_event(src_ip, dst_ip, src_port, dst_port, protocol, packet_count, byte_count, flag, label="BENIGN"):
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": src_port,
        "dst_port": dst_port,
        "protocol": protocol,
        "packet_count": packet_count,
        "byte_count": byte_count,
        "duration": round(random.uniform(0.01, 2.5), 3),
        "flag": flag,
        "label": label
    }

def generate_normal_event():
    src_ip = random.choice(NORMAL_IPS)
    dst_ip = random.choice(SERVERS)
    dst_port = random.choice(COMMON_PORTS)
    protocol = "TCP" if dst_port in [80, 443, 22, 3306] else "UDP"
    packets = random.randint(5, 30)
    bytes_sent = packets * random.randint(64, 1200)
    return make_event(
        src_ip=src_ip,
        dst_ip=dst_ip,
        src_port=random.randint(30000, 65000),
        dst_port=dst_port,
        protocol=protocol,
        packet_count=packets,
        byte_count=bytes_sent,
        flag=random.choice(["ACK", "PSH", "FIN"]),
        label="BENIGN"
    )

def generate_ddos_burst(attacker_ip="198.51.100.42", burst_count=150):
    """Generates an intense volumetric flood from one source IP."""
    print(f"\n[!] INJECTING DDoS BURST: {burst_count} events from {attacker_ip}...")
    events = []
    for _ in range(burst_count):
        events.append(make_event(
            src_ip=attacker_ip,
            dst_ip="10.0.0.5",
            src_port=random.randint(1024, 65535),
            dst_port=80,
            protocol="TCP",
            packet_count=random.randint(500, 2000),
            byte_count=random.randint(40000, 150000),
            flag="SYN",
            label="DDoS_FLOOD"
        ))
    return events

def generate_port_scan(attacker_ip="203.0.113.88", num_ports=60):
    """Generates sequential port scan across multiple destination ports."""
    print(f"\n[!] INJECTING PORT SCAN: Probing {num_ports} ports from {attacker_ip}...")
    events = []
    ports = random.sample(range(1, 1024), min(num_ports, 1000))
    for p in ports:
        events.append(make_event(
            src_ip=attacker_ip,
            dst_ip="10.0.0.10",
            src_port=random.randint(40000, 60000),
            dst_port=p,
            protocol="TCP",
            packet_count=1,
            byte_count=54,
            flag="SYN",
            label="PORT_SCAN"
        ))
    return events

def run_socket_server(port=9999, rate=10, inject_interval=25):
    """
    Runs a TCP socket server streaming JSON-delimited network events to connected clients (like Spark).
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))
    server.listen(1)
    print(f"[*] Network Event Streamer listening on {HOST}:{port}")
    print(f"[*] Connect Apache Spark Streaming or netcat: nc localhost {port}")

    while True:
        client_sock, client_addr = server.accept()
        print(f"[+] Client connected from {client_addr}. Commencing event stream...")
        counter = 0
        try:
            while True:
                counter += 1
                # Periodically inject attacks for demonstration
                if inject_interval > 0 and counter % (inject_interval * rate) == 0:
                    attack_choice = random.choice(["ddos", "portscan"])
                    if attack_choice == "ddos":
                        burst = generate_ddos_burst()
                    else:
                        burst = generate_port_scan()
                    for ev in burst:
                        client_sock.sendall((json.dumps(ev) + "\n").encode("utf-8"))
                        time.sleep(0.01)
                else:
                    event = generate_normal_event()
                    client_sock.sendall((json.dumps(event) + "\n").encode("utf-8"))
                    time.sleep(1.0 / rate)
        except (BrokenPipeError, ConnectionResetError):
            print(f"[-] Client {client_addr} disconnected.")
            client_sock.close()
        except KeyboardInterrupt:
            print("\nStopping socket server...")
            client_sock.close()
            break
    server.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live Network Event Stream Simulator")
    parser.add_argument("--port", type=int, default=9999, help="TCP port to stream events to (default: 9999)")
    parser.add_argument("--rate", type=int, default=10, help="Events per second (default: 10)")
    parser.add_argument("--inject-interval", type=int, default=20, help="Seconds between automated attack injections")
    args = parser.parse_args()

    run_socket_server(port=args.port, rate=args.rate, inject_interval=args.inject_interval)
