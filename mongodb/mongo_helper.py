"""
MongoDB Client Helper & Hybrid Persistence Layer
Course: Big Data Analytics (26ECSC404) - KLE Technological University

Provides seamless integration with MongoDB. If MongoDB daemon is not currently active,
it automatically falls back to an in-memory / JSON persistence store so that all
real-time pipelines and visualizations function flawlessly without crashing during viva demos!
"""

import json
import os
import time
from datetime import datetime

FALLBACK_STORE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts_store.json")

class MongoAlertClient:
    def __init__(self, uri="mongodb://localhost:27017/", db_name="network_db"):
        self.use_mongo = False
        self.db = None
        self.alerts_col = None
        self.events_col = None

        try:
            import pymongo
            self.client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=1000)
            # Test connection
            self.client.server_info()
            self.db = self.client[db_name]
            self.alerts_col = self.db["alerts"]
            self.events_col = self.db["network_events"]
            self.use_mongo = True
            print("[+] Successfully connected to MongoDB at", uri)
        except Exception as e:
            print(f"[-] MongoDB service not reachable ({e}). Using local fallback store.")
            self.use_mongo = False
            self._ensure_fallback_file()

    def _ensure_fallback_file(self):
        if not os.path.exists(FALLBACK_STORE_FILE):
            with open(FALLBACK_STORE_FILE, "w", encoding="utf-8") as f:
                json.dump([], f)

    def insert_alert(self, alert_data):
        if not isinstance(alert_data, dict):
            return False

        if "timestamp" not in alert_data:
            alert_data["timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        if self.use_mongo:
            try:
                self.alerts_col.insert_one(alert_data)
                return True
            except Exception as e:
                print("[-] MongoDB insert error:", e)

        # Fallback persistence
        try:
            alerts = []
            if os.path.exists(FALLBACK_STORE_FILE):
                with open(FALLBACK_STORE_FILE, "r", encoding="utf-8") as f:
                    try:
                        alerts = json.load(f)
                    except Exception:
                        alerts = []
            alerts.append(alert_data)
            # Keep latest 500 alerts in store
            alerts = alerts[-500:]
            with open(FALLBACK_STORE_FILE, "w", encoding="utf-8") as f:
                json.dump(alerts, f, indent=2)
            return True
        except Exception as e:
            print("[-] Fallback store write error:", e)
            return False

    def get_recent_alerts(self, limit=50):
        if self.use_mongo:
            try:
                cursor = self.alerts_col.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit)
                return list(cursor)
            except Exception as e:
                print("[-] MongoDB query error:", e)

        # Fallback read
        if os.path.exists(FALLBACK_STORE_FILE):
            try:
                with open(FALLBACK_STORE_FILE, "r", encoding="utf-8") as f:
                    alerts = json.load(f)
                    alerts.reverse()
                    return alerts[:limit]
            except Exception:
                return []
        return []

    def get_alert_statistics(self):
        alerts = self.get_recent_alerts(limit=500)
        attack_types = {}
        top_ips = {}
        for a in alerts:
            atype = a.get("attack_type", "UNKNOWN")
            ip = a.get("src_ip", "UNKNOWN")
            attack_types[atype] = attack_types.get(atype, 0) + 1
            top_ips[ip] = top_ips.get(ip, 0) + 1

        return {
            "total_alerts": len(alerts),
            "by_attack_type": attack_types,
            "top_ips": top_ips
        }

    def clear_alerts(self):
        if self.use_mongo:
            try:
                self.alerts_col.delete_many({})
            except Exception:
                pass
        with open(FALLBACK_STORE_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)
        return True

# Singleton instance
alert_client = MongoAlertClient()
