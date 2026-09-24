"""
Apache Spark Streaming & Real-Time Anomaly Detection Engine
Course: Big Data Analytics (26ECSC404) - KLE Technological University
Aligns with PI-3.2.2.1 (Sources, Sinks, Windows, Transformations)
and PI-3.3.2.1 (Tumbling vs Sliding Window Trade-Offs)
"""

import sys
import os
import time
import json
import socket
from datetime import datetime, timedelta
from collections import defaultdict, deque

# Add parent path to load mongo helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from mongodb.mongo_helper import alert_client

# Thresholds for Anomaly Detection
DDoS_REQUEST_THRESHOLD_10S = 60       # Requests per 10s tumbling window
PORTSCAN_PORT_THRESHOLD_30S = 12       # Unique destination ports probed within 30s sliding window
BRUTEFORCE_THRESHOLD_30S = 25         # Failed/PSH auth attempts against ports 22/3306

class StreamAnomalyDetector:
    def __init__(self, host="127.0.0.1", port=9999):
        self.host = host
        self.port = port
        
        # In-memory sliding window buffer: deque of (event_time, event_dict)
        self.sliding_buffer = deque()
        self.sliding_window_sec = 30
        self.slide_step_sec = 5
        
        # Tumbling window accumulator: reset every 10 seconds
        self.tumbling_start = time.time()
        self.tumbling_window_sec = 10
        self.tumbling_requests = defaultdict(int)
        self.tumbling_bytes = defaultdict(int)

    def process_event(self, event):
        now = time.time()
        src_ip = event.get("src_ip", "UNKNOWN")
        dst_port = int(event.get("dst_port", 0))
        byte_count = int(event.get("byte_count", 0))

        # -------------------------------------------------------------
        # 1. TUMBLING WINDOW (10 seconds, Non-overlapping)
        # Fast volumetric burst detection
        # -------------------------------------------------------------
        if (now - self.tumbling_start) >= self.tumbling_window_sec:
            # Evaluate Tumbling Window Aggregation
            for ip, count in self.tumbling_requests.items():
                if count >= DDoS_REQUEST_THRESHOLD_10S:
                    alert = {
                        "alert_id": f"ALT-DDoS-{int(now)}-{ip.replace('.', '_')}",
                        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        "src_ip": ip,
                        "attack_type": "DDoS_FLOOD",
                        "metric_count": count,
                        "threshold": DDoS_REQUEST_THRESHOLD_10S,
                        "window_type": "TUMBLING_10S",
                        "severity": "HIGH",
                        "details": f"Volumetric flood: {count} requests in 10s tumbling window."
                    }
                    print(f"\n[ALERT - TUMBLING WINDOW] {alert['attack_type']} from {ip} (Count: {count})")
                    alert_client.insert_alert(alert)

            # Reset tumbling window
            self.tumbling_start = now
            self.tumbling_requests.clear()
            self.tumbling_bytes.clear()

        # Accumulate in current tumbling window
        self.tumbling_requests[src_ip] += 1
        self.tumbling_bytes[src_ip] += byte_count

        # -------------------------------------------------------------
        # 2. SLIDING WINDOW (30s window, 5s evaluation)
        # Stateful multi-attribute analysis (Port Scan & Brute Force)
        # -------------------------------------------------------------
        self.sliding_buffer.append((now, event))

        # Evict events outside 30-second window
        while self.sliding_buffer and (now - self.sliding_buffer[0][0]) > self.sliding_window_sec:
            self.sliding_buffer.popleft()

        # Check sliding metrics every slide step
        if not hasattr(self, "_last_slide_eval") or (now - self._last_slide_eval) >= self.slide_step_sec:
            self._last_slide_eval = now
            self._evaluate_sliding_window()

    def _evaluate_sliding_window(self):
        # Aggregate across 30s sliding buffer
        ip_probed_ports = defaultdict(set)
        ip_auth_attempts = defaultdict(int)

        for ts, ev in self.sliding_buffer:
            ip = ev.get("src_ip")
            port = ev.get("dst_port")
            ip_probed_ports[ip].add(port)
            if port in [22, 3306, 21]:
                ip_auth_attempts[ip] += 1

        # Check port scan rule
        for ip, ports in ip_probed_ports.items():
            if len(ports) >= PORTSCAN_PORT_THRESHOLD_30S:
                alert = {
                    "alert_id": f"ALT-SCAN-{int(time.time())}-{ip.replace('.', '_')}",
                    "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "src_ip": ip,
                    "attack_type": "PORT_SCAN",
                    "metric_count": len(ports),
                    "threshold": PORTSCAN_PORT_THRESHOLD_30S,
                    "window_type": "SLIDING_30S",
                    "severity": "MEDIUM",
                    "details": f"Stealth Port Scan: {len(ports)} distinct ports probed in 30s sliding window."
                }
                print(f"\n[ALERT - SLIDING WINDOW] {alert['attack_type']} from {ip} (Probed: {len(ports)} ports)")
                alert_client.insert_alert(alert)

        # Check brute force rule
        for ip, attempts in ip_auth_attempts.items():
            if attempts >= BRUTEFORCE_THRESHOLD_30S:
                alert = {
                    "alert_id": f"ALT-AUTH-{int(time.time())}-{ip.replace('.', '_')}",
                    "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "src_ip": ip,
                    "attack_type": "BRUTE_FORCE",
                    "metric_count": attempts,
                    "threshold": BRUTEFORCE_THRESHOLD_30S,
                    "window_type": "SLIDING_30S",
                    "severity": "HIGH",
                    "details": f"Brute Force attack: {attempts} auth attempts against administrative services in 30s."
                }
                print(f"\n[ALERT - SLIDING WINDOW] {alert['attack_type']} from {ip} (Auth Attempts: {attempts})")
                alert_client.insert_alert(alert)

    def start_streaming(self):
        print("==================================================================")
        print(" APACHE SPARK / STREAMING ANOMALY DETECTION ENGINE")
        print(" Connecting to Event Stream at {}:{}".format(self.host, self.port))
        print(" Tumbling Window: 10s (Flood Detection)")
        print(" Sliding Window: 30s Window / 5s Slide (Port Scan & Brute Force)")
        print(" Output Sink: MongoDB (network_db.alerts)")
        print("==================================================================")

        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.host, self.port))
                print(f"[+] Connected to network event generator on {self.host}:{self.port}!")
                buffer = ""
                while True:
                    data = s.recv(4096).decode("utf-8")
                    if not data:
                        break
                    buffer += data
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        if line.strip():
                            try:
                                ev = json.loads(line)
                                self.process_event(ev)
                            except json.JSONDecodeError:
                                pass
            except ConnectionRefusedError:
                print(f"[-] Waiting for event generator at {self.host}:{self.port}... (retrying in 3s)")
                time.sleep(3)
            except KeyboardInterrupt:
                print("\nStreaming engine stopped.")
                break
            except Exception as e:
                print(f"[-] Stream connection error: {e}. Reconnecting...")
                time.sleep(3)

if __name__ == "__main__":
    detector = StreamAnomalyDetector(host="127.0.0.1", port=9999)
    detector.start_streaming()
