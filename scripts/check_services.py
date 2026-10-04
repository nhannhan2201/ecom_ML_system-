"""Service Health and Readiness Checker.

Audits all Docker containers and network ports for the e-commerce ML platform,
displaying a clear status table and returning exit code 0 if healthy.
"""

import socket
import sys
import time
import urllib.request


SERVICES = [
    ("MinIO S3 API", "localhost", 9000, "TCP/S3", None),
    ("MinIO Console", "http://localhost:9001", None, "HTTP", "http://localhost:9001"),
    ("PostgreSQL DWH", "localhost", 5432, "TCP/SQL", None),
    ("Redis Online Store", "localhost", 6379, "TCP/RESP", None),
    ("Kafka Message Broker", "localhost", 9092, "TCP/Kafka", None),
    ("Flink JobManager", "http://localhost:8081", None, "HTTP", "http://localhost:8081/overview"),
    ("Airflow Webserver", "http://localhost:8080", None, "HTTP", "http://localhost:8080/health"),
    ("DataHub Frontend", "http://localhost:9002", None, "HTTP", "http://localhost:9002"),
    ("DataHub GMS API", "http://localhost:8089", None, "HTTP", "http://localhost:8089/health"),
]


def check_tcp(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def check_http(url: str, timeout: float = 3.0) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "HealthCheck/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status < 500
    except Exception:
        return False


def main():
    print("=" * 75)
    print("  SERVICE HEALTH & READINESS AUDIT")
    print("=" * 75)
    print(f"{'Service':<25} | {'Type':<8} | {'Target Endpoint':<26} | {'Status'}")
    print("-" * 75)

    all_passed = True
    for name, host_or_url, port, protocol, http_check_url in SERVICES:
        ok = False
        target = host_or_url if protocol == "HTTP" else f"{host_or_url}:{port}"
        for attempt in range(4):
            if protocol == "HTTP":
                ok = check_http(http_check_url or host_or_url)
            else:
                ok = check_tcp(host_or_url, port)
            if ok:
                break
            time.sleep(2)

        status_str = "[OK]" if ok else "[FAIL]"
        if not ok:
            all_passed = False
        print(f"{name:<25} | {protocol:<8} | {target:<26} | {status_str}")

    print("-" * 75)
    if all_passed:
        print("Result: ALL SERVICES ARE HEALTHY AND ACCESSIBLE.")
        print("=" * 75)
        sys.exit(0)
    else:
        print("Result: SOME SERVICES FAILED TO RESPOND. CHECK DOCKER LOGS.")
        print("=" * 75)
        sys.exit(1)


if __name__ == "__main__":
    main()
